from elb import ELB
import xxhash
from flask import Flask, request
from datetime import datetime
import time
import sys
import boto3
from apscheduler.schedulers.background import BackgroundScheduler
import http.client

def main(instance_id):
  global nodes, users_session, is_instance_synced
  nodes = {}
  users_session = {}
  is_instance_synced = False

  elb = ELB(instance_id)
  elb.register_instance_in_elb(instance_id)

  def get_live_node_list():
    healthy, _ = elb.get_targets_status()
    return healthy

  def sync_live_nodes():
    print("Syncing live nodes from the load balancer", file=sys.stderr)  # Printing to stderr for seeing this message during the Flask server is running
    global nodes, is_instance_synced
    updated_live_nodes = get_live_node_list()  # find live nodes list
    session = boto3.Session(region_name="us-east-1")
    ec2_resource = session.resource(service_name="ec2")  # for getting all instances list
    instances_list = ec2_resource.instances.all()
    nodes = {}

    for id in updated_live_nodes:
      for instance in instances_list:
        if id == instance.id:
          nodes[id] = instance.public_ip_address

    is_instance_synced = True
    print(f'Healthy nodes - {str(nodes)}', file=sys.stderr)

  sched = BackgroundScheduler()
  t = datetime.utcnow()
  time.sleep(60 - t.second)  # Waiting until the end of the current minute - in order to perform the sync cycles in the exact time between all instances
  sync_live_nodes()
  sched.add_job(sync_live_nodes, 'interval', seconds=60)  # Add cron job - every 1 minute we'll perform nodes sync
  sched.start()

  app = Flask(__name__)

  @app.route('/health-check')
  def health_check():
      return 'Instance is healthy', 200

  @app.route('/get')
  def get():
    global is_instance_synced, nodes, users_session, instance_id
    if not is_instance_synced:
      return "Not all instances are yet syncronized, please wait less than 1 minute", 500
    user_id = request.args.get("user_id")
    is_piped = request.args.get("is_piped")  # Will be None in case of regular ELB requests
    node, alt_node = get_node_couple(user_id)

    if instance_id != node and instance_id != alt_node:
      if is_piped is not None and is_piped == 'true':  # In case we didn't find the data in node and alt_node - we pipe the 'get' request between all nodes one after each other
        (data, status_code) = get_item(user_id)
        if not is_empty_data(data):  # We didn't find the item in our current instance (and it's not the node & alt_node that the data belongs to)
          next_node = get_next_node_by_current_node(instance_id)
          next_node_data = pipe_request('get', next_node, user_id)
          return next_node_data, 200 if not is_empty_data(next_node_data) else 204
        else:  # We find the item in this instance (The data should be belong to node & alt_node and wasn't found there)
          return data, status_code

      # In this case the original request from the elb got directly to this instance and it's not 'node' or 'alt_node'
      node_data = pipe_request('get', node, user_id)
      alt_node_data = pipe_request('get', alt_node, user_id)
      returned_data = node_data if not is_empty_data(node_data) else alt_node_data
      return returned_data, 200 if not is_empty_data(returned_data) else 204

    elif instance_id == node and is_piped is not None and is_piped == 'true': # Piped the request for the proper 'node' (OR - the request passed all over the nodes and got to the last node)
      return get_item(user_id)

    elif instance_id == alt_node and is_piped is not None and is_piped == 'true': # The request piped to 'alt_node' instance in order to look for the data there
      (data, status_code) = get_item(user_id)
      if is_empty_data(data):  # If the data isn't in 'alt_node' - the request will be piped in a circular way in all nodes until we reach 'node' instance
        next_node = get_next_node_by_current_node(instance_id)
        next_node_data = pipe_request('get', next_node, user_id)
        return next_node_data, 200 if not is_empty_data(next_node_data) else 204
      else:
        return data, status_code

    elif instance_id == node and is_piped is None:  # In this case the original request from the elb got directly to 'node' instance
      (data, status_code) = get_item(user_id)
      if is_empty_data(data) and instance_id != alt_node:  # Looking for the data in alt_node, and if it's not there also - it will piped to the next, and next... nodes until we find / finish
        alt_node_data = pipe_request('get', alt_node, user_id)
        return alt_node_data, 200 if not is_empty_data(alt_node_data) else 204
      else:
        return data, status_code

    elif instance_id == alt_node and is_piped is None:  # In this case the original request from the elb got directly to 'alt_node' instance
      (data, status_code) = get_item(user_id)
      if is_empty_data(data):  # Looking for the data in 'node', and if it's not there also - it will piped to the next, and next... nodes until we find / finish
        node_data = pipe_request('get', node, user_id)
        if is_empty_data(node_data):
          next_node = get_next_node_by_current_node(instance_id)
          next_node_data = pipe_request('get', next_node, user_id)
          return next_node_data, 200 if not is_empty_data(next_node_data) else 204
        return node_data, 200 if is_empty_data(node_data) else 204
      else:
        return data, status_code

  @app.route('/put')
  def put():
    global is_instance_synced, nodes, users_session, instance_id
    if not is_instance_synced:
      return "Not all instances are yet syncronized, please wait less than 1 minute", 500
    user_id = request.args.get("user_id")
    is_piped = request.args.get("is_piped")
    data = request.get_data()
    node, alt_node = get_node_couple(user_id)

    status_code_node = 500
    status_code_alt_node = 500
    if instance_id != node and (is_piped is None or (is_piped is not None and is_piped == 'false')):  # In case not 'node' instance gets the original request - we put it also in 'node' instance
      status_code_node = pipe_request('put', node, user_id, data)
    elif instance_id == node:
      put_item(user_id, data)
      status_code_node = 200
    if instance_id != alt_node and (is_piped is None or (is_piped is not None and is_piped == 'false')):  # In case not 'alt_node' instance gets the original request - we put it also in 'alt_node' instance
      status_code_alt_node = pipe_request('put', alt_node, user_id, data)
    elif instance_id == alt_node and instance_id != node:  # We are in 'alt_node' but no in 'node' instance
      put_item(user_id, data)
      status_code_alt_node = 200

    returned_status_code = 200 if status_code_node == 200 or status_code_alt_node == 200 else 500
    return user_id, returned_status_code

  def get_node_couple(key):
    global nodes
    key_v_node_id = xxhash.xxh64(key).intdigest() % 1024  # calc hash for key
    # get node and alternative node that hold (or should hold) key's data
    instances = list(nodes.keys())
    node = instances[key_v_node_id % len(instances)]
    alt_node = instances[(key_v_node_id + 1) % len(instances)]
    return node, alt_node

  def get_next_node_by_current_node(instance_id):
    global nodes
    index = 0
    instances = list(nodes.keys())
    while(len(instances) == 0):  # If we got here right in the middle of the nodes syncing
      time.sleep(0.2)
      instances = list(nodes.keys())
    for id in instances:
      if id == instance_id:
        return instances[(index + 1) % len(instances)]
      index = index + 1

  def is_empty_data(data):
    return data == 'No data' or data == ''

  def pipe_request(method, instance_id, key, data = None):
    global nodes
    conn = http.client.HTTPConnection(f'{nodes[instance_id]}:8080')
    headers = {'Content-type': 'application/json'}
    try:
      if data:
        conn.request('GET', f'/{method}?user_id={key}&is_piped=true', data, headers=headers)
      else:
        conn.request('GET', f'/{method}?user_id={key}&is_piped=true', headers=headers)
      res = conn.getresponse()
      if method == 'get':
        return res.read().decode("utf-8")
      else:  # put
        return res.status
    except:  # Handling sudden shutdown of an instance that we piped the request to (or a network problem)
      if method == 'get':
        return 'No data'
      else:  # put
        return 500

  def get_item(key):
    global users_session
    data = 'No data'
    if key in users_session:
      data = users_session[key]
      status_code = 200
    if data == 'No data':
      status_code = 204
    return data, status_code

  def put_item(key, data):
    global users_session
    users_session[key] = data


  app.run(host="0.0.0.0", port=8080)


if __name__=="__main__":
  if len(sys.argv) < 2:
    raise Exception('No instance id provided!')
  else:
    instance_id = sys.argv[1]
    main(instance_id)

