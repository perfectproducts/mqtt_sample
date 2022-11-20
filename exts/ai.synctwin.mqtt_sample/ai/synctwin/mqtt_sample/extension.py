import omni.ext
import omni.ui as ui
from paho.mqtt import client as mqtt_client
import random
from pxr import Usd, Kind, UsdGeom, Sdf, Gf, Tf


class SyncTwinMqttSampleExtension(omni.ext.IExt):

    def load_usd_model(self):
        # load our forklift
        print("loading model...")
        self._usd_context.open_stage("http://omniverse-content-production.s3-us-west-2.amazonaws.com/Assets/DigitalTwin/Assets/Warehouse/Equipment/Forklifts/Forklift_A/Forklift_A01_PR_V_NVD_01.usd")        
        
    
        
    def on_startup(self, ext_id):
        print("[ai.synctwin.mqtt_sample] ai synctwin mqtt_sample startup")
        # init data         
        self.mqtt_topic_model = ui.SimpleStringModel("synctwin/mqtt_demo/forklift/fork_level")
        self.mqtt_broker_host_model = ui.SimpleStringModel("test.mosquitto.org")
        self.mqtt_broker_port_model = ui.SimpleStringModel("1883")
        
        self.mqtt_value_model = ui.SimpleFloatModel(0)
        self.mqtt_value_model.add_value_changed_fn(self.on_mqtt_value_changed)

        self.mqtt_connected_model = ui.SimpleBoolModel(False)
        self.target_prim_model = ui.SimpleStringModel("/World/Geometry/SM_Forklift_Fork_A01_01")
        self.current_fork_level = 0 

        # init ui 
        self._usd_context = omni.usd.get_context()
        self._window = ui.Window("SyncTwin MQTT Sample", width=300, height=350)
        with self._window.frame:
            with ui.VStack():

                ui.Button("load model",clicked_fn=self.load_usd_model)

                ui.Label("MQTT Broker")
                with ui.HStack():
                    
                    ui.StringField(self.mqtt_broker_host_model)
                    
                    ui.StringField(self.mqtt_broker_port_model, width=ui.Percent(20))

                ui.Label("Topic")
                ui.StringField(self.mqtt_topic_model)
                
                ui.Label("Target Prim")
                ui.StringField(self.target_prim_model)

                ui.Label("Value")
                ui.StringField(self.mqtt_value_model)

                self.status_label = ui.Label("- not connected -")
                
                ui.Button("connect MQTT", clicked_fn=self.connect_mqtt)

        
                    
        # we want to know when model changes 
        self._sub_stage_event = self._usd_context.get_stage_event_stream().create_subscription_to_pop(
                self._on_stage_event                
            )

        # find our xf prim if model already present 
        self.find_xf_prim()

        # and we need a callback on each frame to update our xf prim 
        self._app_update_sub = omni.kit.app.get_app().get_update_event_stream().create_subscription_to_pop(
                self._on_app_update_event, name="synctwin.mqtt_sample._on_app_update_event"
            )   
        
    # called on every frame, be careful what to put there 
    def _on_app_update_event(self, evt):
        # if we have found the transform lets update the translation 
        if self.xf:            
            self.xf.ClearXformOpOrder()
            self.xf.AddTranslateOp().Set(Gf.Vec3f(0, 0, self.current_fork_level))       

    # called on load 
    def _on_stage_event(self, event):
        if event.type == int(omni.usd.StageEventType.OPENED): 
            print("opened new model")
            self.find_xf_prim()

    # our model callback
    def on_mqtt_value_changed(self, model):        
        self.current_fork_level = model.get_value_as_float()
    
    # find the prim to be transformed 
    def find_xf_prim(self):
        # get prim from input 
        stage = self._usd_context.get_stage()
        prim = stage.GetPrimAtPath(self.target_prim_model.get_value_as_string())
        
        self.xf = UsdGeom.Xformable(prim)
        
        if self.xf:
            msg = "found xf."
        else:
            msg = "## xf not found."
        self.status_label.text = msg 
        print(msg)

    # connect to mqtt broker 
    def connect_mqtt(self):

        # this is called when a message arrives 
        def on_message(client, userdata, msg):
            msg_content = msg.payload.decode()

            print(f"Received `{msg_content}` from `{msg.topic}` topic")
            # userdata is self 
            userdata.mqtt_value_model.set_value(float(msg_content))

        # called when connection to mqtt broker has been established 
        def on_connect(client, userdata, flags, rc):
            print(f">> connected {client} {rc}")
            if rc == 0:
                self.status_label.text = "Connected to MQTT Broker!"
                # connect to our topic 
                topic = userdata.mqtt_topic_model.get_value_as_string()
                print(f"subscribing topic {topic}")
                client.subscribe(topic)
            else:
                self.status_label.text = f"Failed to connect, return code {rc}"

        # let us know when we've subscribed 
        def on_subscribe(client, userdata, mid, granted_qos):
            print(f"subscribed {mid} {granted_qos}")
        
        # now connect broker
        broker = self.mqtt_broker_host_model.get_value_as_string()
        port = self.mqtt_broker_port_model.get_value_as_int()
        
        client_id = f'python-mqtt-{random.randint(0, 1000)}'
        # Set Connecting Client ID
        client = mqtt_client.Client(client_id)
        
        client.user_data_set(self)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_subscribe = on_subscribe
        client.connect(broker, port)
        client.loop_start()
        return client

    def on_shutdown(self):
        print("[ai.synctwin.mqtt_sample]  shutdown")
        self.client = None
        self._app_update_sub = None
