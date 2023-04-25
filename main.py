import os
import argparse
import datetime, calendar
import multiprocessing as mp
from databricks_cli.oauth.oauth import check_and_refresh_access_token
from databricks_cli.configure.provider import update_and_persist_config, ProfileConfigProvider
from databricks_cli.configure.config import _get_api_client as get_api_client
from databricks_cli.clusters.api import ClusterService
from databricks_cli.cluster_policies.api import ClusterPolicyApi

def get_email(profile):
    config = ProfileConfigProvider(profile).get_config()
    email = config.username
    return email


def get_profiles():
    print("Finding Databricks profiles")
    home_dir = os.getenv("HOME")
    with open(f"{home_dir}/.databrickscfg", "r") as f:
        list_profile = [str(i).strip().replace("[","").replace("]","") for i in f.readlines() if str(i).strip().startswith("[") and str(i).strip().endswith("]")]
    print("Profiles Found: %d" % len(list_profile))
    return list_profile

def get_api(profile):
    print("Configuring Authentication for CLI")
    config = ProfileConfigProvider(profile).get_config()
    if config.host and config.token and config.refresh_token:
        config.token, config.refresh_token, updated = \
            check_and_refresh_access_token(config.host, config.token, config.refresh_token)
        if updated:
            update_and_persist_config(profile, config)
    api_client = get_api_client(config, "clusters")
    return api_client


def get_personal_cluster_policies(api):
    cp = ClusterPolicyApi(api)
    policies = cp.list_cluster_policies()
    personal_cluster_policies = [p['policy_id'] for p in policies['policies'] if 'policy_family_id' in p.keys() if
           p['policy_family_id'] == 'personal-vm']
    return personal_cluster_policies


def delete_cluster(api, list_of_clusters):
    cs = ClusterService(api)
    for cluster in list_of_clusters:
        try:
            cs.delete_cluster(cluster_id=cluster[0])
        except:
            print("Failed to delete cluster {}".format(cluster))


def list_personal_clusters(api, created_after):
    cs = ClusterService(api)
    list_clusters = cs.list_clusters()['clusters']
    list_policies = get_personal_cluster_policies(api)
    #personal_compute_clusters = [cluster for cluster in list_clusters if 'single_user_name' in cluster.keys()]
    personal_compute_clusters = [cluster for cluster in list_clusters if 'policy_id' in cluster.keys() if cluster['policy_id'] in list_policies]
    final_list = []
    for cluster in personal_compute_clusters:
        cluster_id = cluster['cluster_id']
        print("Processing Cluster ID - {}".format(cluster_id))
        cluster_name = cluster['cluster_name']
        creator = cluster['creator_user_name']
        # events = cs.get_events(cluster_id, limit=500)['events']
        # try:
        #     creation_time = [event['timestamp'] for event in events if event['type'] == 'CREATING'][-1]
        # except:
        #     creation_time = [event['timestamp'] for event in events][-1]
        events = cs.get_events(cluster_id, limit=1, event_types=['CREATING'], order='ASC')['events']
        creation_time = [event['timestamp'] for event in events][0]
        if creation_time > created_after:
            readable_ts = datetime.datetime.utcfromtimestamp(creation_time/1000).strftime('%Y-%m-%d %H:%M:%S')
            final_list.append([cluster_id, cluster_name, creator, readable_ts])
    return final_list


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Finds all Personal Compute Clusters')
    #parser.add_argument('--workspaces', help='List of workspaces to run this for')
    parser.add_argument('--email', help='Admin Email address for Workspace paths')
    parser.add_argument('--profile', help='Databricks profile to use to connect to Workspace')
    args = parser.parse_args()
    email, profiles = args.email, str(args.profile).split(",")
    date_from = datetime.datetime(2023, 4, 11, 0, 0, 0)
    date_from_unix = calendar.timegm(date_from.timetuple()) * 1000
    #profiles = ['DEFAULT']
    #profiles = get_profiles()
    for profile in profiles:
        print("=" * 100)
        print(f"Starting execution for profile {profile}")
        print("=" * 100)
        api = get_api(profile)
        print(f"Start time - {datetime.datetime.now()}")
        personal_clusters = list_personal_clusters(api, date_from_unix)
        print(f"End time - {datetime.datetime.now()}")
        print("Personal Clusters created after 11 Apr 2023:")
        print(personal_clusters)
        with open(f"{profile}_personal_clusters.txt", "w+") as f:
            f.write("cluster_id, cluster_name, creator, created_ts\n")
            f.writelines(map((lambda x: ", ".join(x) + "\n"), personal_clusters))
        #delete_cluster(api, personal_clusters)

