import multiprocessing
import threading
import cv2
import queue
import time

class FrameReader:
    '''class to read frames from an RTSP stream using OpenCV and multiprocessing. 
    Restarts the frame reader process if no frame is captured within a specified timeout period.
    cummuncaiton between the main process and the frame reader process is done using a multiprocessing.Queue.
    usage: Create an instance of the FrameReader class and call the run method.
    args: rtsp_url: str: RTSP URL of the stream
          timeout: int: Timeout period in seconds to wait for a frame
          restart_delay: int: Delay in seconds before restarting the frame reader process
    methods: frame_reader: Method to read frames from the RTSP stream
                frame_reader_process: Method to create a process to read frames from the RTSP stream
                start_frame_reader: Method to start the frame reader process
                stop_frame_reader: Method to stop the frame reader process
                get_frame: Method to get a frame from the frame queue
                run: Method to run the frame reader process
     
    FrameReader instance will read frames from the RTSP stream and display them using OpenCV.
    returns: None'''

    def __init__(self, rtsp_url, timeout=5, restart_delay=5):
        self.rtsp_url = rtsp_url
        self.timeout = timeout
        self.restart_delay = restart_delay
        self.frame_queue = multiprocessing.Queue(maxsize=1)
        self.stop_event = multiprocessing.Event()
        self.process = None
        self.last_frame_time = time.time()

    def frame_reader(self, cap, frame_queue, stop_event):
        while not stop_event.is_set():
            ret, frame = cap.read()
            if ret:
                if not frame_queue.empty():
                    try:
                        frame_queue.get_nowait()  # Remove the old frame
                    except queue.Empty:
                        pass
                frame_queue.put(frame)
            else:
                print("Failed to read frame")

    def frame_reader_process(self, rtsp_url, frame_queue, stop_event):
        cap = cv2.VideoCapture(rtsp_url)
        if not cap.isOpened():
            print("Error: Cannot open RTSP stream")
            return
        reader_thread = threading.Thread(target=self.frame_reader, args=(cap, frame_queue, stop_event))
        reader_thread.start()
        reader_thread.join()
        cap.release()

    def start_frame_reader(self):
        self.process = multiprocessing.Process(target=self.frame_reader_process, args=(self.rtsp_url, self.frame_queue, self.stop_event))
        self.process.start()

    def stop_frame_reader(self):
        if self.process is not None:
            self.stop_event.set()
            self.process.terminate()
            self.process.join()
            self.stop_event.clear()

    def get_frame(self):
        try:
            frame = self.frame_queue.get(timeout=self.timeout)
            self.last_frame_time = time.time()
            return frame
        except queue.Empty:
            print("No frame captured within the timeout period")
            if time.time() - self.last_frame_time > self.timeout:
                print("Restarting frame reader process")
                self.stop_frame_reader()
                time.sleep(self.restart_delay)  # Delay before restarting the process
                self.start_frame_reader()
                self.last_frame_time = time.time()
            return None

    def run(self):
        self.start_frame_reader()
        try:
            while True:
                frame = self.get_frame()
                if frame is not None:
                    cv2.imshow('Frame', frame)
                    if cv2.waitKey(1) & 0xFF == ord('q'):
                        break
                else:
                    print("No frame captured")
        finally:
            self.stop_frame_reader()
            cv2.destroyAllWindows()
def main():
    # Define the RTSP URL
    rtsp_url = 'rtsp://admin:ThomasDudley1920@10.0.3.171:554/Streaming/Channels/102/'

    # Create a FrameReader instance and run it
    frame_reader = FrameReader(rtsp_url)
    frame_reader.run()

if __name__ == '__main__':
    main()
    