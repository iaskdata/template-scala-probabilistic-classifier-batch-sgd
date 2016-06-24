"""
Import sample data for recommendation engine
"""

import predictionio
import argparse
import random

RATE_ACTIONS_DELIMITER = ","
SEED = 3

def import_events(client, file):
  f = open(file, 'r')
  random.seed(SEED)
  count = 0
  print "Importing data..."
  for line in f:
    data = line.rstrip('\r\n').split(RATE_ACTIONS_DELIMITER)
    client.create_event(
      event="loan",
      entity_type="user",
      entity_id=0,
      target_entity_type=data[6],
      target_entity_id=0,
      #"loan_amnt","term","int_rate","emp_length","home_ownership","annual_inc","purpose","addr_state","dti","delinq_2yrs","revol_util","total_acc","bad_loan","longest_credit_length","verification_status"

      properties= { 
        "loan_amnt" : data[0],
        "term" : data[1],
        "int_rate" : data[2],
        "emp_length" : data[3],
        "home_ownership" : data[4],
        "annual_inc" : data[5],
        "purpose" : data[6],
        "addr_state" : data[7],
        "dti" : data[8],
        "delinq_2yrs" : data[9],
        "revol_util" : data[10],
        "total_acc" : data[11],
        "bad_loan" : data[12],
        "longest_credit_length" : data[13],
        "verification_status" : data[14],
      }
    )
    count += 1
  f.close()
  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for recommendation engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")
  parser.add_argument('--file', default="./data/sample_movielens_data.txt")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=5,
    qsize=500)
  import_events(client, args.file)