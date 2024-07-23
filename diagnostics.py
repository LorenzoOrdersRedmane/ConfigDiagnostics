
import json
import argparse
from mcase import Datalist
import os

def main(single_config, config_folder):
    # technically bad for to open a file without closing it, but this is a just quick python script
    if single_config is not None:
        datalist_jsons = json.load(open(single_config, encoding="utf-8"))
    if config_folder is not None:
        datalist_jsons = [json.load(open(os.path.join(config_folder, f), encoding='utf-8'))[0] for f in os.listdir(config_folder) if f.endswith(".json")]

    datalists = Datalist.create_datalists_from_jsons(datalist_jsons)
    for datalist_name in datalists:
        errors = datalists[datalist_name].fetch_error_messages()
        if len(errors) > 0:
            print("Datalist:", datalist_name)
            print("\n".join(datalists[datalist_name].fetch_error_messages()))
            print()
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser("")
    config_source = parser.add_mutually_exclusive_group()
    config_source.add_argument("--single-config")
    config_source.add_argument("--config-folder")
    args = parser.parse_args()
    main(args.single_config, args.config_folder)