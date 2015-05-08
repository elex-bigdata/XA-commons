import threading
import sys
import os
import Queue

queue_load_to_hdfs = Queue.Queue(0)
local_path_deleted = "/data2/deleted/"
hdfs_path_del_idmap = "/user/hadoop/deleted_idmap/"

special_projects = ["sof-wpm", "sof-zip", "sof-windowspm", "quick-start","sof-ient", "sof-newgdp", "sof-newgdppop", "sof-yacnvd",
                    "i18n-status", "web337", "lightning-speedial", "sof-dsk", "sof-installer", "omiga-plus", "webssearches", "sweet-page", "infospace", "delta-homes"]

thread_num = 2
try_times = 3

class loadLocalFileToHDFSWorker(threading.Thread):
    def __init__(self, host, queue_load_to_hdfs, date):
        self.host = host
        self.queue_load_to_hdfs = queue_load_to_hdfs
        self.date = date
        threading.Thread.__init__(self)

    def run(self):
        while True:
            pid = self.queue_load_to_hdfs.get()
            if pid == 'exit':
                self.queue_load_to_hdfs.task_done()
                break

            local_path = local_path_deleted + pid + "/" + date + ".txt"
            hdfs_path = hdfs_path_del_idmap + pid + "/" + date + "/" + self.host + ".log"
            command = "hadoop fs -copyFromLocal " + local_path + " " + hdfs_path

            local_size = os.path.getsize(local_path)
            hdfs_size = 0
            print "Begin to excute " + command
            times = try_times
            while times > 0 :
                if local_size != 0:
                    status = os.system(command)
                    ret = os.popen('hadoop fs -du ' + hdfs_path).readlines()
                    if ret != [] :
                        hdfs_size = int(ret[0].split(' ')[0])

                if local_size == 0 or (status == 0 and local_size == hdfs_size):
                    break
                os.system("hadoop fs -rm " + hdfs_path)
                times = times - 1

            self.queue_load_to_hdfs.task_done()
            print pid + "_" + self.host + ".log : local_size " + str(local_size) + ",hdfs_size " + str(hdfs_size)
            if times == 0 :
                print 'Excute ' + command + ' failed'

if __name__ == '__main__':
    host = sys.argv[1]
    date = sys.argv[2]
    pids = set(special_projects)

    total_dump_task = 0
    for pid_each in pids:
        mkdir_command = "hadoop fs -mkdir -p deleted_idmap/" + pid_each + "/" + date
        os.system(mkdir_command)
        queue_load_to_hdfs.put(pid_each)
        total_dump_task += 1
    print 'Total task num: ' + str(total_dump_task) + "\n"

    for i in range(thread_num):
        loadLocalFileToHDFSWorker(host, queue_load_to_hdfs, date).start()