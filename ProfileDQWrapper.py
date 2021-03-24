from resources.thread.ProfileDQScheduler import ProfileDQScheduler
import threading
from resources.utilities.ExceptionLog import ExceptionLog

class ProfileDQWrapper:
    def __init__(self, input_data, postgre_alchemy):
        self.inputdata = input_data #[{'sourceid': '1', 'loginusername': 'anki', 'tables_files': ['tab1', 'tab2', 'tab3']}, {}, {}]
        self.postgre_alchemy = postgre_alchemy


    def scheduleprofiledq(self):
        try:
            batchid = self.postgre_alchemy.getbatchid()[0]
            print(self.inputdata)
            self.inputdata['batchid'] = batchid
            self.inputdata['runid'] = 1
            self.postgre_alchemy.insert_batch(self.inputdata)
            self.postgre_alchemy.insert_batch_audit(self.inputdata)

            ProfileDQScheduler(batchid, self.postgre_alchemy).schedule()
            return batchid
        except:
            ExceptionLog().log()
            raise
        #dq = DQScheduler(batchid, self.postgre_alchemy)
        #threading.Thread(target=profile.run).start()
        #threading.Thread(target=dq.run).start()