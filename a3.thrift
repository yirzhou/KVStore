service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void putToReplica(1: string key, 2: string value);
  void migrateToReplica(1: map<string, string> dict);
  void ping();
  void assignPrimary();
}
