import json
import os
import time
from collections import defaultdict

class Time:
    """Mimics rospy.Time / babyros.Time"""
    def __init__(self, secs=0, nsecs=0):
        self.secs = secs
        self.nsecs = nsecs
        
    def to_sec(self):
        return self.secs + self.nsecs / 1e9

    @staticmethod
    def now():
        t = time.time()
        return Time(int(t), int((t - int(t)) * 1e9))
        
    def __lt__(self, other):
        return self.to_sec() < other.to_sec()

class Bag:
    """Mimics rosbag.Bag"""
    def __init__(self, filename, mode='r'):
        self.filename = filename
        self.mode = mode
        self.topics_info = defaultdict(lambda: {'count': 0, 'type': 'unknown'})
        self.start_time = None
        self.end_time = None
        
        if mode == 'w':
            self.file = open(filename, 'wb')  # Overwrite
        elif mode == 'a':
            self.file = open(filename, 'ab')  # Append
        elif mode == 'r':
            if not os.path.exists(filename):
                raise IOError(f"File {filename} not found")
            self.file = open(filename, 'rb')
            self._build_index()
        else:
            raise ValueError("Mode must be 'r', 'w', or 'a'")

    def _build_index(self):
        """Reads the file to build topic info and time bounds."""
        self.file.seek(0)
        for line in self.file:
            try:
                record = json.loads(line.decode('utf-8').strip())
                topic = record['topic']
                t = Time(record['time']['secs'], record['time']['nsecs'])
                
                self.topics_info[topic]['count'] += 1
                if 'type' in record:
                    self.topics_info[topic]['type'] = record['type']
                    
                if self.start_time is None or t < self.start_time:
                    self.start_time = t
                if self.end_time is None or self.end_time < t:
                    self.end_time = t
            except Exception:
                continue

    def write(self, topic, msg, t=None):
        if self.mode not in ('w', 'a'):
            raise IOError("Bag must be opened in 'w' or 'a' mode to write")
        
        if t is None:
            t = Time.now()
            
        # Serialize message (handles dicts, objects with __dict__, or primitives)
        if hasattr(msg, '__dict__'):
            msg_dict = msg.__dict__
        elif isinstance(msg, dict):
            msg_dict = msg
        else:
            msg_dict = {'data': msg}
            
        record = {
            'topic': topic,
            'time': {'secs': t.secs, 'nsecs': t.nsecs},
            'type': type(msg).__name__,
            'msg': msg_dict
        }
        
        line = json.dumps(record).encode('utf-8') + b'\n'
        self.file.write(line)
        
        # Update live index
        self.topics_info[topic]['count'] += 1
        self.topics_info[topic]['type'] = type(msg).__name__
        
        if self.start_time is None or t < self.start_time:
            self.start_time = t
        if self.end_time is None or self.end_time < t:
            self.end_time = t

    def read_messages(self, topics=None, start_time=None, end_time=None):
        if self.mode != 'r':
            raise IOError("Bag must be opened in 'r' mode to read")
            
        self.file.seek(0)
        for line in self.file:
            try:
                record = json.loads(line.decode('utf-8').strip())
            except:
                continue
                
            rec_topic = record['topic']
            rec_time = Time(record['time']['secs'], record['time']['nsecs'])
            
            if topics and rec_topic not in topics:
                continue
            if start_time and rec_time < start_time:
                continue
            if end_time and end_time < rec_time:
                continue
                
            # Yields exactly like rosbag: (topic, msg, time)
            yield rec_topic, record['msg'], rec_time

    def get_type_and_topic_info(self, topic_filters=None):
        class TopicInfo:
            def __init__(self, msg_type, message_count):
                self.msg_type = msg_type
                self.message_count = message_count
                
        class Info:
            def __init__(self):
                self.topics = {}
                
        info = Info()
        for topic, data in self.topics_info.items():
            if topic_filters and topic not in topic_filters:
                continue
            info.topics[topic] = TopicInfo(data['type'], data['count'])
            
        return info

    def close(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()