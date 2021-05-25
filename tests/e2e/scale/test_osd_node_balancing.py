"""
Test osd node balancing by adding nodes and osds and checking their distribution
"""
import logging
from functools import reduce
from uuid import uuid4
from ocs_ci.framework.testlib import scale, ignore_leftovers
from ocs_ci.ocs import constants, defaults, machine, ocp
from ocs_ci.ocs.exceptions import CephDetectVersionPodFound
from ocs_ci.ocs.node import (
    add_new_node_and_label_it,
    get_osd_running_nodes,
    get_nodes,
    wait_for_nodes_status,
)
from ocs_ci.ocs.perfresult import PerfResult
from ocs_ci.ocs.perftests import PASTest
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.ocs.resources.pod import wait_for_storage_pods, get_all_pods
from ocs_ci.utility import templating
from ocs_ci.utility.retry import retry
from ocs_ci.utility.utils import ceph_health_check

FINAL_REPORT = "Final Report"
INITIAL_SETUP = "Initial Setup"
MAX_NODE_COUNT = 9
MAX_OSDS_PER_NODE = 3
START_NODE_NUM = 3
REPLICA_COUNT = 3
OSD_LIMIT_AT_START = MAX_OSDS_PER_NODE * START_NODE_NUM
MAX_TIMES_ADDED = 3


def add_some_worker_nodes(elastic_info):
    """
    Add three worker nodes to the storage cluster.  Update the report file.
    """
    osd_running_nodes = get_osd_running_nodes()
    logging.info(f"OSDs are running on nodes {osd_running_nodes}")
    # Get the machine name using the node name
    machine_names = [
        machine.get_machine_from_node_name(osd_node) for osd_node in osd_running_nodes
    ]
    logging.info(f"{osd_running_nodes} associated machines are {machine_names}")
    # Get the machineset name using machine name
    machineset_names = [
        machine.get_machineset_from_machine_name(machine_name)
        for machine_name in machine_names
    ]
    # This next section makes new nodes on which OSD pods can run
    for mnames in range(0, REPLICA_COUNT):
        add_new_node_and_label_it(machineset_names[mnames], mark_for_ocs_label=True)
        wait_for_nodes_status()
    collect_stats(f"{REPLICA_COUNT} nodes have been added", elastic_info)


def increase_osd_capacity(elastic_info):
    """
    Add three osds to the cluster.  Update the report file.
    """
    # This next section increases the number of OSDs on each node by one.
    osd_size = storage_cluster.get_osd_size()
    logging.info(f"Adding a new set of OSDs. osd size = {osd_size}")
    storagedeviceset_count = storage_cluster.add_capacity(osd_size)
    retry(
        (CephDetectVersionPodFound),
        tries=6,
        delay=10,
        backoff=1,
    )(find_problem_pods)()
    logging.info(f"storagedeviceset is {storagedeviceset_count}")
    wait_for_storage_pods(timeout=600)
    logging.info("Successfully added a new set of OSDs")
    ceph_health_check(tries=30, delay=60)
    collect_stats("OSD capacity has been increased", elastic_info)


def find_problem_pods():
    """
    Check for rook-ceph-detect-version in running pods.  Throw
    CephDetectVersionPodFound if there is still one around.
    This avoids a timing bug with terminating rook-ceph-detect-version pods
    without resorting to sleeping.
    """
    scan_pod_obj = get_all_pods(namespace=defaults.ROOK_CLUSTER_NAMESPACE)
    name_of_pods = [x.name for x in scan_pod_obj]
    plist = [x.startswith("rook-ceph-detect-version") for x in name_of_pods]
    found_detect_version = reduce((lambda x, y: x | y), plist)
    if found_detect_version:
        raise CephDetectVersionPodFound
    return True


def collect_stats(action_text, elastic_info):
    """
    Write the current configuration information into the REPORT file.
    This information includes the osd, nodes and which osds are on which
    nodes.  The minimum and maximum numbers of osds per node are also
    computed and saved.  If this is the final call to collect_stats
    (action_text parameter is FINAL_REPORT), then the data collected
    in the REPORT file is also displayed in the log.

    Args:
        action_text -- Title of last action taken
                (usually adding nodes or adding osds)
        elastic_info -- ElasticData object
    """
    output_info = {"title": action_text}
    pod_obj = ocp.OCP(
        kind=constants.POD, namespace=constants.OPENSHIFT_STORAGE_NAMESPACE
    )
    osd_list = pod_obj.get(selector=constants.OSD_APP_LABEL)["items"]
    node_stats = {}
    for osd_ent in osd_list:
        osd_node = osd_ent["spec"]["nodeName"]
        if osd_node in node_stats:
            node_stats[osd_node].append(osd_ent)
        else:
            node_stats[osd_node] = [osd_ent]
    osds_per_node = []
    for entry in node_stats:
        osds_per_node.append(len(node_stats[entry]))
    wnodes = get_nodes(constants.WORKER_MACHINE)
    for wnode in wnodes:
        if wnode.name not in node_stats:
            osds_per_node.append(0)
    maxov = max(osds_per_node)
    minov = min(osds_per_node)
    this_skew = maxov - minov
    logging.info(f"Skew found is {this_skew}")
    output_info["osds"] = osd_list
    output_info["worker_nodes"] = wnodes
    output_info["pairings"] = {}
    for entry in osd_list:
        output_info["pairings"][entry["metadata"]["name"]] = entry["spec"]["nodeName"]
    output_info["maxov"] = maxov
    output_info["minov"] = minov
    output_info["skew_value"] = this_skew
    balanced = True
    if this_skew > 1 and maxov > MAX_OSDS_PER_NODE:
        balanced = False
    elastic_info.add_key(elastic_info.record_counter, output_info)
    elastic_info.log_recent_activity()
    elastic_info.record_counter += 1
    assert balanced, "OSDs are not balanced"


class ElasticData(PerfResult):
    """
    Wrap PerfResult and keep track of a counter to be used as
    an index into the table data saved.
    """

    def __init__(self, uuid, crd):
        super(ElasticData, self).__init__(uuid, crd)
        self.index = "test_osd_node_balancing"
        self.new_index = "test_osd_node_balancing_new"
        self.record_counter = 0

    def log_recent_activity(self):
        new_data = self.results[self.record_counter]
        logging.info(new_data["title"])
        logging.info("pairings:")
        for entry in new_data["pairings"]:
            logging.info(f"     {entry} -- {new_data['pairings'][entry]}")
        logging.info(f"maxov: {new_data['maxov']}")
        logging.info(f"minov: {new_data['minov']}")
        logging.info(f"skew_value: {new_data['skew_value']}")


@ignore_leftovers
@scale
class Test_Osd_Balance(PASTest):
    """
    There is no cleanup code in this test because the final
    state is much different from the original configuration
    (several nodes and osds have been added)
    """

    def test_osd_balance(self, es):
        """
        Current pattern is:
            add 6 osds (9 total, 3 nodes)
            add 3 nodes
            add 9 osds (18 total, 6 nodes)
            add 3 nodes
            add 9 osds (27 total, 9 nodes)
        """
        crd_data = templating.load_yaml(constants.OSD_SCALE_BENCHMARK_YAML)
        our_uuid = uuid4().hex
        ltext = ["first", "second", "third"]
        self.elastic_info = ElasticData(our_uuid, crd_data)
        self.elastic_info.es_connect()
        collect_stats(INITIAL_SETUP, self.elastic_info)
        for node_grp in range(0, MAX_TIMES_ADDED):
            strt = 0
            if node_grp > 0:
                add_some_worker_nodes(self.elastic_info)
            else:
                strt = 1
            for osd_cnt in range(strt, MAX_OSDS_PER_NODE):
                cntval = ltext[osd_cnt]
                logging.info(f"Adding {cntval} set of osds to nodes")
                increase_osd_capacity(self.elastic_info)
        collect_stats(FINAL_REPORT, self.elastic_info)
