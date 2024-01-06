import local_index
import datetime
import json
import logging
import platform
import os
import subprocess
import re

class IdalekoMacMachineConfig:
    '''
    This is the analog to the version in the config script.  In this class we
    look for and load the captured machine configuration data.  We have this
    separation because different platforms require different steps to gather up
    machine information.
    '''

    def __init__(self, config_dir : str):
        assert platform.system() == 'Darwin', 'This class is for Darwin'
        assert config_dir is not None, f'No config directory specified: {config_dir}'
        assert os.path.exists(config_dir), 'Config directory does not exist'
        self.config_dir = config_dir
       
        self.config_data = {
            'MachineUuid': self.__get_mac_machine_id()
        }

    def __get_mac_machine_id(self):
        # TODO: parse the output of "ioreg -l | grep IOPlatformSerialNumber"
        try:
            result = subprocess.run(['system_profiler', 'SPHardwareDataType'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            output = result.stdout

            # Use regular expression to find the serial number
            match = re.search(r'Serial Number \(system\): (.+)', output)
            if match:
                serial_number = match.group(1).strip()
                return serial_number
            else:
                print("couldn't get the serial number on this machine")
                exit(1)
        except Exception as e:
            print(f"Error: {e}")
            exit(1)
            
    def __load__config_data__(self):
        with open(self.config_file, 'rt', encoding='utf-8-sig') as fd:
            self.config_data = json.load(fd)

    def get_config_data(self):
        if self.config_data is None:
            self.__load__config_data__()
        return self.config_data

def construct_mac_output_file_name(path : str, configdir = './config'):
    maccfg = IdalekoMacMachineConfig(config_dir=configdir)
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return f'mac-local-fs-data-machine={maccfg.get_config_data()["MachineUuid"]}-date={timestamp}.json'

def build_stat_dict(name: str, root : str, config : IdalekoMacMachineConfig, last_uri = None, last_drive = None) -> tuple:
    file_path = os.path.join(root, name)
    try:
        stat_data = os.stat(file_path)
    except:
        logging.warning(f'Unable to stat {file_path}')
        return None
    stat_dict = {key : getattr(stat_data, key) for key in dir(stat_data) if key.startswith('st_')}
    stat_dict['is_dir'] = os.path.isdir(file_path)
    stat_dict['file'] = name
    stat_dict['path'] = root
    stat_dict['URI'] = file_path
    return (stat_dict, file_path, last_drive)

def walk_files_and_directories(path: str, config : IdalekoMacMachineConfig) -> list:
    files_data = []
    dirs_data = []
    last_drive = None
    last_uri = None
    for root, dirs, files in os.walk(path):
        for name in files + dirs:
            entry = build_stat_dict(name, root, config, last_uri, last_drive)
            if entry is not None:
                files_data.append(entry[0])
                last_uri = entry[1]
                last_drive = entry[2]
    return dirs_data + files_data

def get_default_index_path():
    return os.path.expanduser("~")

def main():
    # Now parse the arguments
    li = local_index.LocalIngest()
    li.add_arguments('--path', type=str, default=get_default_index_path(), help='Path to index')
    args = li.parse_args()
    print(args)
    machine_config = IdalekoMacMachineConfig(config_dir=args.confdir)
    # now I have the path being parsed, let's figure out the drive GUID
    li.set_output_file(construct_mac_output_file_name(args.path))
    args = li.parse_args()
    data = walk_files_and_directories(args.path, machine_config)
    # now I just need to save the data
    output_file = os.path.join(args.outdir, args.output).replace(':', '_')
    with open(output_file, 'wt') as fd:
        json.dump(data, fd, indent=4)

if __name__ == "__main__":
    main()