import cv2
import time
import numpy as np
import mediapipe as mp
import keras

from cameras import CVCamera, PICamera
from config import Config, CameraConfig, Colors
from config import ConfigMediapipeDetector
from gui import WindowMessage
from landmarksLib import draw_landmarks_on_image

ON_RASPBERRY_PI = False
MODEL_PATH = "models/3_21_2_CNN1.keras"

# Instantiate the configuration
# Classes to be recognized; ATENTION: 'None' class must be the last one; the others must be specified in the order they were trained (alphabetical order)
classes=['cold', 'down', 'drunk', 'hot', 'stop', 'swipe', 'up', 'None']
window_title = "Hand gestures recognition demonstrator"
colors = Colors()
colors.SelectRandomColorFromListForClasses(classes)
config = Config(classes=classes, use_landmarks = True)
cam_config = CameraConfig(FPS=15, resolution='highres')

debug_HAR = True
only_landmarks = False
pred_mappings=classes
landmark_values = [[0, 0] for _ in range(21)]

def normalize_from_0_landmark(data):
    """
    Normaliza coordenadas (T, 21, 2) restando el landmark 0 (muñeca)
    y escalando según el máximo desplazamiento en el frame.
    """
    data = np.array(data, dtype=float)  # asegurar tipo float
    normalized = np.copy(data)

    # Si llega con dimensión de batch (1, 3, 21, 2), eliminamos la primera
    if len(normalized.shape) == 4:
        normalized = normalized[0]

    for i in range(normalized.shape[0]):  # recorrer frames
        wrist = normalized[i, 0, :]  # landmark 0 (x, y)
        normalized[i] -= wrist  # restar la muñeca a todos los puntos
        max_val = np.max(np.abs(normalized[i]))
        if max_val > 0:
            normalized[i] /= max_val  # escalar a [-1, 1]
    return np.expand_dims(normalized, axis=0)  # devolver con batch


def main():
    # Create the detector
    detector = ConfigMediapipeDetector('./models/hand_landmarker.task')

    # Start camera, use CVCamera if working on a laptop and PICamera in case you are working on a Raspberry PI
    if ON_RASPBERRY_PI:
        cam = PICamera(recording_res=cam_config.resolution)
        sense_hat = SenseHat()
        sense_hat.set_rotation(180)
    else:
        cam = CVCamera(recording_res=cam_config.resolution, index_cam=0)
        sense_hat = None
    
    
    # Load model to use
    print("Loading model...")
    model = keras.models.load_model(MODEL_PATH)
    print("Model loaded!")
    print(model.input_shape)

    # Start camera
    cam.start()
    
    now = 0
    last = 0
    num_frames = 0 # Number of frames processed
    pred = 'None'

    frames_buffer = []

    while True:
        image = cam.read_frame()
        if image is None:
            print("Waiting for camera input")
            continue

        now = time.time()
        num_frames += 1

        image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=image_rgb)
        detection_result = detector.detect(mp_image)

        if detection_result.hand_landmarks:
            image, landmark_values = draw_landmarks_on_image(image, detection_result)

            landmarks_nparray = np.array(landmark_values)  # (21, 2)
            frames_buffer.append(landmarks_nparray)

            # Mantén solo los últimos 5 frames
            if len(frames_buffer) > 3:
                frames_buffer.pop(0)

            # Solo predice si ya tenemos 5 frames acumulados
            if len(frames_buffer) == 3 and now - last > 0.25:
                seq_array = np.array(frames_buffer)               # (3, 21, 2)
                seq_array = normalize_from_0_landmark(seq_array)  # (1, 3, 21, 2)

                predictions = model(seq_array)
                pred = pred_mappings[np.argmax(predictions)]
                conf = np.max(predictions)

                if debug_HAR:
                    print('Prediction:', pred, 'Confidence:', conf)

                last = time.time()


        else:
            pred = 'None'
            conf = 1.0
            frames_buffer.clear()
        
        class_msgs = WindowMessage(
            txt1 = "Predicted class: " + pred + " (%0.2f)" % conf, pos1 = (10, cam_config.resolution[1]-20), col1 = colors.GetColorForClass(pred),
            txt2 = "", pos2 = (0, 0), col2 = colors.color['black'],
            txt3 = "", pos3 = (0, 0), col3 = colors.color['black'])

        class_msgs.ShowWindowMessages(image)

        cv2.imshow(window_title, image)

        # Press 'q' to quit the program
        key = cv2.waitKey(int(1 / cam_config.FPS * 1000)) & 0xFF
        
        if key ==  ord('q'):
            if ON_RASPBERRY_PI:
                sense_hat.clear()
            break
    
    # Release resources
    cam.stop()
    exit()

if __name__ == "__main__":
    main()