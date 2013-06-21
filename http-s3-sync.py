#!/usr/bin/env python
import os,sys
import httplib2
import workerpool
import boto
from boto.s3.key import Key
from urllib import urlretrieve
from urlparse import urlparse 

# limit in memory transfer to 10Mb
size_limit=1024 * 1024 * 10
logname=sys.argv[0].strip('.py').lstrip('/') + '.log'
log = open(logname,'w')
s3 = boto.connect_s3()
bucket = s3.get_bucket('')
prefix = ''
verbose=False

def logger(l):
	if verbose:
		sys.stdout.write(l)
	else:
		log.write(l)

class S3Sync(workerpool.Job):    
    "Job for downloading and coping the image to s3"
    def __init__(self, url):
        self.url = url 
    

    def run(self):
	try: 
		dest_host = bucket.get_website_endpoint()
		u = urlparse(self.url)
		keyname = u.path
		h = httplib2.Http()
		resp_origin,c_origin = h.request(u.geturl(),'HEAD')
		resp_dest,c_dest = h.request('http://%s%s' % (dest_host,u.path),'HEAD')
		if resp_origin['status'] != resp_dest['status'] :
			if resp_origin['content-length'] > size_limit:
				# big file, save to disk
				logger('%s is larger then limit: %s, saving to disk\n' % (u.geturl(),resp_origin['content-length']))
				save_path= '/tmp/' + os.path.basename(u.path) 
				urlretrieve(u.geturl(),save_path)
				k  = Key(bucket)
				k.set_metadata("Content-Type",resp_origin['content-type'])
				k.name = prefix + keyname         
				k.set_contents_from_file(open(save_path))
				k.set_acl('public-read')
				os.remove(save_path)
				logger('%s syncronized\n' % k.generate_url(0,query_auth=False,force_http=True))
			else:
				resp, content = h.request(self.url)
				k  = Key(bucket)
				k.set_metadata("Content-Type",resp_origin['content-type'])
				k.name = prefix + keyname         
				k.set_contents_from_string(content)
				k.set_acl('public-read')
				logger('%s syncronized\n' % k.generate_url(0,query_auth=False,force_http=True))
		else:
			logger('http://%s%s in sync\n' % (dest_host,u.path))
			
	except Exception,e:	
		logger('could not copy url %s - %s\n' % (self.url,e))

pool = workerpool.WorkerPool(size=10)
for url in open(sys.argv[1]):
    job = S3Sync(url.strip())
    pool.put(job)

pool.shutdown()
pool.wait()
log.close()
