from datetime import datetime
import json
import os
from threading import Thread

# macros
ERROR = -1
TIMEOUT = -2
FAILED = -3

# globals
corpus = open('corpus.txt', 'r')
startedReqs = {}
completedReqs = {}
results = []
killMonitor = False

def monitor(logdir='logs', lat_thresh=60):
  while not killMonitor:
    startedReqsCopy = startedReqs
    for request in startedReqsCopy:
      # check whether request has completed
      if request in completedReqs:
        continue

      start = startedReqsCopy[request]
      fname = '{dir}/{req}.log'.format(dir=logdir, req=request)

      # check for error
      with open(fname + '.err') as errfile:
        if len(errfile.readlines()):
          completedReqs[request] = ERROR
          continue

      # check for legit complete
      with open(fname + '.out') as outfile:
        lines = outfile.readlines()
        if len(lines):
          line = lines[-1]
          try:
            retval = line.split()[0]
          except ValueError:
            completedReqs[request] = FAILED
            continue

          completedReqs[request] = datetime.utcnow()

      # request has not returned yet. check for timeout
      if (datetime.utcnow() - start).seconds > lat_thresh:
        completedReqs[request] = TIMEOUT
        continue

    for request in completedReqs:
      if request not in startedReqsCopy:
        # run has seen that it has completed
        # accumulated its info and removed it from startedReqs
        completedReqs.pop(request)

# inputs: [{'key':<>, 'arg':<>, 'func':<>}]
def run(inputs, logdir='logs', num_parallel=32, num_req=65000):
  if len(inputs) <= num_parallel:
    num_parallel = len(inputs) - 1

  started = 0
  finished = 0
  nextInputIndex = 0
  activeInputIndexes = [-1] * num_parallel
  activeReqInfo = [{}] * num_parallel

  while finished < num_req:
    for loadgenId in range(num_parallel):
      activeReq = activeInputIndexes[loadgenId] # currently running req's index in inputs

      if (started < num_req and activeReq < 0):
        # no active request in this load-generator + creation limit not reached
        # create new request
        newreq = inputs[nextInputIndex]
        activeInputIndex[loadgenId] = nextInputIndex

        # find next INACTIVE input
        while (nextInputIndex in activeInputIndex):
          nextInputIndex = (nextInputIndex + 1) % len(inputs)

        # create the request
        outfile = '{dir}/{req}.log'.format(dir=logdir, req=started)
        key = newreq['key']
        func = newreq['func']
        arg = newreq['arg']

        cmd = "inv -r faasmcli/faasmcli invoke ndp " + func + " -i '" + key + " " + arg + "' 1>" + outfile + ".out 2>" + outfile + ".err &"
        os.system(cmd)

        activeReqInfo[loadgenId] = {
          'reqID' : started,
          'key' : key,
          'func' : func,
          'arg' : arg,
          'loadgenID' : loadgenId
          'start' : datetime.utcnow()
          'end' : -1
          'latency' : -1
          'timedout' : False
          'failed' : False
          'error' : False
        }

        startedReqs.append(started)
        startedReqs[started] = activeReqInfo[loadgenId]['start']
        started += 1

      elif activeReq >= 0:
        # check if request has completed
        activeReqId = activeReqInfo[loadgenId]['reqID']
        completedReqsCopy = completedReqs

        if activeReqId in completedReqsCopy:
          # request has completed
          startedReqs.pop(activeReqId)

          completedAt = completedReqsCopy[activeReqId]
          if completedAt == ERROR:
            activeReqInfo[loadgenId]['error'] = True
          elif completedAt == TIMEOUT:
            activeReqInfo[loadgenId]['timedout'] = True
          elif completedAt == FAILED:
            activeReqInfo[loadgenId]['failed'] = True
          else:
            activeReqInfo[loadgenId]['end'] = completedAt
            activeReqInfo[loadgenId]['latency'] = completedAt - activeReqInfo[loadgenId]['start']
        
        activeInputIndexes[loadgenId] = -1
        results.append(activeReqInfo[loadgenId])
        activeReqInfo[loadgenId] = {}
        finished += 1

  killMonitor = True


lines = corpus.readlines()
inputsFull = [json.loads(line.replace("'", "\"")) for line in lines]

inputs = [{
            'key':'f' + str(inp['id']), 
            'arg': inp['author'].strip().split()[0], 
            'func': 'grep'
          } for inp in inputsFull[100:1200] if inp['language'] == 'English'] +
          [{
            'key':'f' + str(inp['id']), 
            'arg': inp['author'].strip().split()[0], 
            'func': 'substr'
          } for inp in inputsFull[120:1400] if inp['language'] == 'English'] +

logdir = 'logs'
num_parallel = 2
num_warm = 4
num_req = 40
lat_thresh = 60


if __name__ == "__main__":
  t1 = Thread(target = run, args = [inputs, logdir, num_parallel, num_req])
  t2 = Thread(target = monitor, args = [logdir, lat_thresh])

  t1.start()
  t2.start()
  t1.join()
  t2.join()

  for res in results:
    print(res)

  # add post-processing to deal with warmup_reqs etc