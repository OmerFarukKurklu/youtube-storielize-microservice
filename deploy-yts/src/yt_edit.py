import subprocess
from pytube import YouTube
import os
import falcon
import boto3
import shutil
import json
import time
from multiprocessing import Process

BUCKET_NAME = [BUCKET_NAME] #CHANGE THIS


CONCAT_FILE_NAME = 'concat.txt'
TEMP_FOLDER = 'youtube'
COMP_FOLDER = 'compfolder'

def download_vid(key_id, video_id):
    url = 'https://www.youtube.com/watch?v=' + video_id
    try:
        vid = YouTube(url)
        file_name = vid.streams.first().download()
    except Exception as e:
        log_update(key_id, '425 Unable to Download Video', 'Error occured while downloading the video. Possible reasons are:\n\t-> Video is forbidden by youtube due copyright laws.\n\t-> URL to video does not work\n['+str(e)+']')
    try:
        vid_raw_name = TEMP_FOLDER + '/' + video_id + '_raw.mp4'
        os.rename(file_name, vid_raw_name)
    except OSError as e:
        log_update(key_id, '501 os.rename FAILURE', 'Could not rename file at download_vid [' + str(e)+']')
    return {'video':vid, 'raw_name':vid_raw_name}

def create_folder(key_id):
    try:
        os.mkdir(TEMP_FOLDER)
        os.mkdir(TEMP_FOLDER + '/'+ COMP_FOLDER)
    except OSError as e:
        log_update(key_id, '502 os.mkdir FAILURE','Could not create necessary folders [' + str(e)+']')

def concat(result_vid_name):
    command = 'ffmpeg -f concat -i '+CONCAT_FILE_NAME+' -c copy ' + result_vid_name
    subprocess.call(command, shell=True)

def upload_to_s3(key_id, result_vid_name):
    try:
        s3 = boto3.resource('s3')
        data = open(result_vid_name, 'rb')
        s3.Bucket(BUCKET_NAME).put_object(Key=result_vid_name, Body=data)
        return BUCKET_NAME
    except Exception as e:
        log_update(key_id,'505 upload_to_s3 FAILURE.', str(e))

def cleanup(key):
    try:
        if os.path.isfile(CONCAT_FILE_NAME):
            os.remove(CONCAT_FILE_NAME)
        shutil.rmtree(TEMP_FOLDER)
    except Exception as e:
        localtime = time.localtime(time.time())
        localtime = str(localtime.tm_year) + '-' + str(localtime.tm_mon) + '-' + str(localtime.tm_mday) + ' ' + str(localtime.tm_hour) + ':'+ str(localtime.tm_min) + ':' + str(localtime.tm_sec) + ' +0' + str(int(- time.timezone / 36))
        with open("yte-logs.json", "r+") as logs_file:
                logs = json.load(logs_file)
                logs[key]['status'] = '506 cleanup FAILURE.'
                logs[key]['message'] = '!!! MANUAL DELETE REQUIRED !!! [' + str(e)+']'
                logs[key]['finished_at'] = localtime
                logs_file.seek(0)
                logs_file.truncate()
                logs_file.write(json.dumps(logs))
                logs_file.close()

def log_create(key):
    localtime = time.localtime(time.time())
    localtime = str(localtime.tm_year) + '-' + str(localtime.tm_mon) + '-' + str(localtime.tm_mday) + ' ' + str(localtime.tm_hour) + ':'+ str(localtime.tm_min) + ':' + str(localtime.tm_sec) + ' +0' + str(int(- time.timezone / 36))
    with open("yte-logs.json", "r+") as logs_file:
        logs = json.load(logs_file)
        logs[key] = {'status':'Waiting','message':'Waiting for proccess to finish','requested_at':localtime, 'finished_at':'-', 'bucket_name':BUCKET_NAME, 'bucket_url':'https://s3.console.aws.amazon.com/s3/buckets/' + BUCKET_NAME}
        logs_file.seek(0)
        logs_file.truncate()
        logs_file.write(json.dumps(logs))
        logs_file.close()

def log_update(key, status, message):
    localtime = time.localtime(time.time())
    localtime = str(localtime.tm_year) + '-' + str(localtime.tm_mon) + '-' + str(localtime.tm_mday) + ' ' + str(localtime.tm_hour) + ':'+ str(localtime.tm_min) + ':' + str(localtime.tm_sec) + ' +0' + str(int(- time.timezone / 36))
    with open("yte-logs.json", "r+") as logs_file:
            logs = json.load(logs_file)
            logs[key]['status'] = status
            logs[key]['message'] = message
            logs[key]['finished_at'] = localtime
            logs_file.seek(0)
            logs_file.truncate()
            logs_file.write(json.dumps(logs))
            logs_file.close()
    cleanup(key)

class Comp(object):

    def split_to_clips (self, video, pref_skip_length, pref_clip_length):
        length = int(video['video'].length)
        skip = 0
        clip_no = 0
        try:
            concat_file = open(CONCAT_FILE_NAME, 'a')
            while skip <= length:
                out_name = TEMP_FOLDER + '/' + COMP_FOLDER +'/out'+str(clip_no)+'.mp4'
                command = 'ffmpeg -ss '+ str(skip) +' -i '+video['raw_name']+' -t '+ str(pref_clip_length) +' ' + out_name
                subprocess.call(command, shell=True)
                concat_file.write('file ' + out_name+'\n')
                skip = skip + int(pref_skip_length)
                clip_no = clip_no + 1
            concat_file.close()
        except Exception as e:
            log_update(video['video'].video_id, '503 split_to_clips FAILURE', str(e))

    def main(self, key_id, vid_id, skip_length, clip_length):

        create_folder(key_id)
        video = download_vid(key_id,key_id)
        self.split_to_clips(video, skip_length, clip_length)
        result_vid_name = TEMP_FOLDER + '/' + key_id + '_comp.mp4'
        concat(result_vid_name)
        upload_to_s3(key_id,result_vid_name)
        log_update(key_id, '200 OK', 'Success')

    def on_get(self, req, resp):
        try:
            vid_id = req.params['id']
            skip_length = req.params['sl']
            clip_length = req.params['cl']
            if vid_id == '' or skip_length == '' or clip_length =='':
                raise ImportError
        except ImportError:
            raise falcon.HTTPInvalidParam('Parameters cannot be blank', '"id","sl","cl"')
        except:
            raise falcon.HTTPMissingParam(param_name = 'id (id: video id), sl (sl: Skip Length, how many seconds to pass between clips), cl (cl: Clip Length, length of a single clip)')
        with open("yte-logs.json", "r+") as logs_file:
            logs = json.load(logs_file)
            logs_file.close
        key = vid_id + '_' + skip_length + '_' +  clip_length
        if key in logs:
            resp.content_type = 'json'
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(logs[key])
            if logs[key]['status'] != '200 OK':
                Process(target=self.main, args=(key, vid_id, skip_length, clip_length)).start()
        else:
            check_vid=YouTube('https://www.youtube.com/watch?v=' + vid_id)
            if int(check_vid.length) > 600:
                raise falcon.HTTPFailedDependency(description='Video is more than 10 mins long')
            log_create(key)
            Process(target=self.main, args=(key, vid_id, skip_length, clip_length)).start()
            resp.content_type = 'json'
            resp.status = falcon.HTTP_200
            body = {'response':'Process started, GET request again to see status.'}
            resp.body = json.dumps(body)

class SmartComp(object):

    def split_to_clips(self, video, pref_skip_length, pref_clip_length):
        length = int(video['video'].length)
        skip = 0
        clip_no = 0
        try:
            concat_file = open(CONCAT_FILE_NAME, 'a')
            out_name = TEMP_FOLDER + '/' + COMP_FOLDER +'/out'+str(clip_no)+'.mp4'
            clip_no = clip_no + 1
            command = 'ffmpeg -ss '+ str(skip) +' -i '+video['raw_name']+' -t '+ '2' +' ' + out_name
            subprocess.call(command, shell=True)
            skip = skip + 2
            concat_file.write('file ' + out_name+'\n')
            while skip <= length - 2:
                out_name = TEMP_FOLDER + '/' + COMP_FOLDER +'/out'+str(clip_no)+'.mp4'
                command = 'ffmpeg -ss '+ str(skip) +' -i '+video['raw_name']+' -t '+ str(pref_clip_length) +' ' + out_name
                subprocess.call(command, shell=True)
                concat_file.write('file ' + out_name+'\n')
                skip = skip + int(pref_skip_length)
                clip_no = clip_no + 1
            out_name = TEMP_FOLDER + '/' + COMP_FOLDER +'/out'+str(clip_no)+'.mp4'
            command = 'ffmpeg -ss '+ str(length-2) +' -i '+video['raw_name']+ ' ' + out_name
            subprocess.call(command, shell=True)
            concat_file.write('file ' + out_name+'\n')
            concat_file.close()
        except Exception as e:
            log_update(video['video'].video_id, '503 split_to_clips FAILURE', str(e))
    
    def find_skip(self,length, pref_clip_length, pref_total_length):
        no_of_clips = pref_total_length / pref_clip_length
        skip_length = length / no_of_clips
        return skip_length

    def main(self, key_id):
        PREF_CLIP_LENGTH = 0.4
        PREF_TOTAL_LENGTH = 10
        create_folder(key_id)
        video = download_vid(key_id,key_id)
        length = video['video'].length
        skip_length = self.find_skip(int(length) ,PREF_CLIP_LENGTH,PREF_TOTAL_LENGTH)
        self.split_to_clips(video, skip_length, PREF_CLIP_LENGTH)
        result_vid_name = TEMP_FOLDER + '/' + key_id + '_smart.mp4'
        concat(result_vid_name)
        upload_to_s3(key_id,result_vid_name)
        log_update(key_id, '200 OK', 'Success')

    def on_get(self, req, resp):
        try:
            key_id = req.params['id']
            if key_id == '':
                raise ImportError
        except ImportError:
            raise falcon.HTTPInvalidParam('Parameter cannot be blank', 'url')
        except:
            raise falcon.HTTPMissingParam(param_name = 'id (ID of video)')
        with open("yte-logs.json", "r+") as logs_file:
            logs = json.load(logs_file)
            logs_file.close
        if key_id in logs:
            resp.content_type = 'json'
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(logs[key_id])
            if logs[key_id]['status'] != '200 OK':
                Process(target=self.main, args=(key_id,)).start()
        else:
            check_vid=YouTube('https://www.youtube.com/watch?v=' + key_id)
            if int(check_vid.length) > 600:
                raise falcon.HTTPFailedDependency(description='Video is more than 10 mins long')
            log_create(key_id)
            Process(target=self.main, args=(key_id,)).start()
            resp.content_type = 'json'
            resp.status = falcon.HTTP_200
            body = {'response':'Process started, GET request again to see status.'}
            resp.body = json.dumps(body)

class Clip(object):

    def main(self, key, vid_id, start, end):
        os.mkdir(TEMP_FOLDER)
        video = download_vid(key, vid_id)
        length = int(end) - int(start)
        result_vid_name = TEMP_FOLDER + '/' + key + '_clip.mp4'
        command = 'ffmpeg -ss '+ str(start) +' -i '+video['raw_name']+' -t '+ str(length) +' ' + result_vid_name
        subprocess.call(command, shell=True)
        upload_to_s3(key,result_vid_name)
        log_update(key, '200 OK', 'Success')

    def on_get(self, req, resp):
        try:
            vid_id = req.params['id']
            start_sec = req.params['s']
            end_sec = req.params['e']
            if id == '' or start_sec == '' or end_sec =='':
                raise ImportError
        except ImportError:
            raise falcon.HTTPInvalidParam('Parameters cannot be blank', '"id","s","e"')
        except:
            raise falcon.HTTPMissingParam(param_name = 'id (id: video id), s (s: Start, start point of the clip, in seconds), e (e: End, end point of the clip, in seconds)')
        with open("yte-logs.json", "r+") as logs_file:
            logs = json.load(logs_file)
            logs_file.close
        key = vid_id + '_' + start_sec + '_' +  end_sec
        if key in logs:
            resp.content_type = 'json'
            resp.status = falcon.HTTP_200
            resp.body = json.dumps(logs[key])
            if logs[key]['status'] != '200 OK':
                Process(target=self.main, args=(key, vid_id, start_sec, end_sec)).start()
        else:
            check_vid=YouTube('https://www.youtube.com/watch?v=' + vid_id)
            if int(check_vid.length) > 600:
                raise falcon.HTTPFailedDependency(description='Video is more than 10 mins long')
            log_create(key)
            Process(target=self.main, args=(key, vid_id, start_sec, end_sec)).start()
            resp.content_type = 'json'
            resp.status = falcon.HTTP_200
            body = {'response':'Process started, GET request again to see status.'}
            resp.body = json.dumps(body)

app = falcon.API()

comp_resource = Comp()
clip_resource = Clip()
smart_resource = SmartComp()

app.add_route('/comp', comp_resource)
app.add_route('/clip', clip_resource)
app.add_route('/smart', smart_resource)