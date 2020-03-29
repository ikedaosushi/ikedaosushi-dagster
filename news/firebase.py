from pathlib import Path
import firebase_admin
from firebase_admin import credentials, firestore, storage

cred = credentials.Certificate(str(Path.home()/".gcp/tech-news.json"))
app = firebase_admin.initialize_app(cred, name='tech-news')

class FirebaseOps:
    def __init__(self):
        self.db = firestore.client(app=app)
        self.bucket = storage.bucket('tech-news-77085.appspot.com', app=app)
        
    def entry_exists(self, entry_id):
        return self.db.collection("entries").document(entry_id).get().exists
    
    def set_entry(self, entry_id, entry):
        self.db.collection("entries").document(entry_id).set(entry)

    def upload_from_string(self, blob_id, string):
        self.bucket.blob(blob_id).upload_from_string(string)

# singleton
firebase_ops = FirebaseOps()