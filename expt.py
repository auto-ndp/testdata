from datetime import datetime
import json
import os
import pycurl
import random
import sys
from threading import Thread

# macros
SUCCESS = "SUCCESS"
RUNNING = "RUNNING"
CREATED = "CREATED"
ERROR = "ERROR"
TIMEOUT = "TIMEOUT"
FAILED = "FAILED"

# globals
corpusFile = open('corpus.txt', 'r')
EXPTTS = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
workerPort = '8080'
workerHost = 'worker'
timeout = 15


def perform_curl_request(request):
  try:
    request.start = datetime.utcnow()
    request.curl.perform()
  except:
    request.status = ERROR
    if (datetime.utcnow() - request.start).seconds >= timeout:
      request.status = TIMEOUT


class Request:
  nextRequestID = 0

  def __init__(self, inp, loadgenID):
    self.requestID = Request.nextRequestID
    Request.nextRequestID += 1

    self.inputKey = inp['key']
    self.inputFunc = inp['func']
    self.inputArg = inp['arg']

    self.loadgenID = loadgenID
    self.start = -1

    self.end = -1
    self.latency = -1
    self.status = CREATED
    self.response = None
    self.curl = None

  def writeback(self, buf):
    if self.status == ERROR or self.status == TIMEOUT:
      return

    self.response = buf.decode('utf-8')
    self.end = datetime.utcnow()
    self.latency = self.end - self.start

    try:
      i = int(self.response.strip().split()[0])
      self.status = SUCCESS
    except:
      self.status = FAILED


  def init_curl(self):
    payload = '{"async": false, "user": "ndp", "function": "' + self.inputFunc + '", "input_data": "' + self.inputKey + ' ' + self.inputArg + '"}'
    url = "http://" + workerHost + ":" + workerPort + "/f/"

    c = pycurl.Curl()
    c.setopt(c.URL, url)
    c.setopt(c.POSTFIELDS, payload)
    c.setopt(c.HTTPHEADER, ["Content-Type: application/json"])
    c.setopt(c.WRITEFUNCTION, self.writeback)
    c.setopt(c.TIMEOUT, timeout)
    self.curl = c

  def show(self):
    if self.status == SUCCESS:
      self.response = self.response.split()
      if len(self.response) > 3:
        self.response = self.response[:3] + ['...']
      self.latency = self.latency.seconds + (self.latency.microseconds / 10**6)

    print("{id} {func} {key} {arg} {status} {latency} {resp}".format(id=self.requestID, func=self.inputFunc, key=self.inputKey, arg=self.inputArg, status=self.status, latency=self.latency, resp=self.response))


def run(exptCfg):
  name = exptCfg['name']
  inputs = exptCfg['inputs']
  num_parallel = exptCfg['num_parallel']
  num_req = exptCfg['num_req']

  print("Starting Expt {name} req={req} par={par}".format(name=name, req=num_req, par=num_parallel))

  finishedRequests = []
  activeRequests = [None] * num_parallel
  nextInputIndex = 0
  threads = []

  sTime = datetime.utcnow()

  while len(finishedRequests) < num_req:
    for loadgenID in range(num_parallel):
      if activeRequests[loadgenID] == None:
        # create new request
        if Request.nextRequestID < num_req:
          request = Request(inputs[nextInputIndex], loadgenID)
          request.init_curl()
          nextInputIndex = (nextInputIndex + 1) % len(inputs)

          t = Thread(target=perform_curl_request, args=[request])
          threads.append(t)
          t.start()
          request.status = RUNNING
          activeRequests[loadgenID] = request

      elif activeRequests[loadgenID].status != RUNNING:
        finishedRequests.append(activeRequests[loadgenID])
        activeRequests[loadgenID] = None

    print("\rStarted:{started}\tCompleted:{completed}\tRemaining:{remaining:05d}".format(started=Request.nextRequestID, completed=len(finishedRequests), remaining=(num_req - Request.nextRequestID)), end='')

  print("\nFinished!")
  for t in threads:
    t.join()

  eTime = datetime.utcnow()

  return finishedRequests, (eTime - sTime).seconds + ((eTime - sTime).microseconds / 10**6)

def postprocess(cfg, results, e2etime):
  statsfile = "logs/stats.{name}.{dt}".format(name=cfg['name'], dt=EXPTTS)
  os.makedirs('logs', exist_ok=True)

  num_err = 0
  num_timeout = 0
  num_failed = 0
  num_succ = 0
  latencies = []

  for res in results:
    res.show()
    if res.status == ERROR:
      num_err += 1
    elif res.status == TIMEOUT:
      num_timeout += 1
    elif res.status == FAILED:
      num_failed += 1
    else:
      num_succ += 1
      latencies.append(res.latency)

  with open(statsfile, 'w') as stats:
    stats.write('REQS {reqs}\n'.format(reqs=cfg['num_req']))
    stats.write('LOADGENS {par}\n'.format(par=cfg['num_parallel']))
    stats.write('SUCCESS {succ}\n'.format(succ=num_succ))
    stats.write('FAILED {fail}\n'.format(fail=num_failed))
    stats.write('TIMEOUT {tmo}\n'.format(tmo=num_timeout))
    stats.write('ERROR {err}\n'.format(err=num_err))
    stats.write('E2E {e2e}\n'.format(e2e=e2etime))
    stats.write('AVGLAT {avg:.06f}\n'.format(avg=(sum(latencies) / num_succ)))
    stats.write('LATENCIES {lat}\n'.format(lat=" ".join([str(l) for l in latencies])))


if __name__ == "__main__":
  corpus = []
  for line in corpusFile.readlines():
    try:
      corpus.append(json.loads(line.replace("'", "\"")))
    except:
      pass

  inputs = []
  for entry in corpus:
    try:
      key = "f" + str(entry["id"])
      arg = entry["author"].strip().split()[0]

      payload = '{"data": "' + key + ' ' + arg + '"}'
      c = pycurl.Curl()
      c.setopt(c.POSTFIELDS, payload)
      assert(entry["language"] == "English")

      inputs.append({'key':key, 'arg':arg, 'func':"substr"})
      inputs.append({'key':key, 'arg':arg, 'func':"grep"})
    except:
      pass

  random.shuffle(inputs)

  CFG_DIFF_OBJ = {'name':'DIFF_OBJ', 'inputs':inputs, 'num_req':100, 'num_parallel':4}
  CFG_SAME_OBJ = {'name':'SAME_OBJ', 'inputs':[inputs[0]], 'num_req':100, 'num_parallel':4}

  cfg = CFG_DIFF_OBJ
  results, e2e = run(cfg)
  postprocess(cfg, results, e2e)
