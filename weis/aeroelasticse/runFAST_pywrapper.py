"""
A basic python script that demonstrates how to use the FST8 reader, writer, and wrapper in a purely
python setting. These functions are constructed to provide a simple interface for controlling FAST
programmatically with minimal additional dependencies.
"""

import os
import shutil
import platform
import multiprocessing as mp
import glob
import copy

from openfast_io.FAST_reader import InputReader_OpenFAST
from openfast_io.FAST_writer import InputWriter_OpenFAST
from weis.aeroelasticse.FAST_wrapper import FAST_wrapper, Turbsim_wrapper, IEC_CoherentGusts
from weis.aeroelasticse.cloud_wrapper import Inductiva_wrapper
from weis.aeroelasticse.calculated_channels import calculate_channels
from pCrunch.io import OpenFASTOutput, OpenFASTBinary, OpenFASTAscii
from pCrunch import LoadsAnalysis, FatigueParams
from weis.aeroelasticse.openfast_library import FastLibAPI

import numpy as np

# Realpath will resolve symlinks
# of_path = os.path.realpath( shutil.which('openfast') )
of_path = os.path.realpath( '/home/mayank/development/env/weis-inductiva/bin/openfast' )
bin_dir  = os.path.dirname(of_path)
lib_dir  = os.path.abspath( os.path.join(os.path.dirname(bin_dir), 'lib') )

mactype = platform.system().lower()
if mactype in ["linux", "linux2"]:
    libext = ".so"
    staticext = ".a"
elif mactype in ["win32", "windows", "cygwin"]: #NOTE: platform.system()='Windows', sys.platform='win32'
    libext = '.dll'
    staticext = ".lib"
elif mactype == "darwin":
    libext = '.dylib'
    staticext = ".a"
else:
    raise ValueError('Unknown platform type: '+mactype)

found = False
for libname in ['libopenfastlib', 'openfastlib']:
    for d in [lib_dir, bin_dir]:
        lib_path = os.path.join(d, libname+libext)
        if os.path.exists(lib_path):
            found = True
            break
    if found:
        break

    
magnitude_channels_default = {
    'LSShftF': ["RotThrust", "LSShftFys", "LSShftFzs"], 
    'LSShftM': ["RotTorq", "LSSTipMys", "LSSTipMzs"],
    'RootMc1': ["RootMxc1", "RootMyc1", "RootMzc1"],
    'RootMc2': ["RootMxc2", "RootMyc2", "RootMzc2"],
    'RootMc3': ["RootMxc3", "RootMyc3", "RootMzc3"],
    'TipDc1': ['TipDxc1', 'TipDyc1', 'TipDzc1'],
    'TipDc2': ['TipDxc2', 'TipDyc2', 'TipDzc2'],
    'TipDc3': ['TipDxc3', 'TipDyc3', 'TipDzc3'],
    'TwrBsM': ['TwrBsMxt', 'TwrBsMyt', 'TwrBsMzt'],
}

fatigue_channels_default = {
    'RootMc1': FatigueParams(slope=10),
    'RootMc2': FatigueParams(slope=10),
    'RootMc3': FatigueParams(slope=10),
    'RootMyb1': FatigueParams(slope=10),
    'RootMyb2': FatigueParams(slope=10),
    'RootMyb3': FatigueParams(slope=10),
    'TwrBsM': FatigueParams(slope=4),
    'LSShftM': FatigueParams(slope=4),
}

# channel_extremes_default = [
#     'RotSpeed',
#     'BldPitch1','BldPitch2','BldPitch3',
#     "RotThrust","LSShftFys","LSShftFzs","RotTorq","LSSTipMys","LSSTipMzs","LSShftF","LSShftM",
#     'Azimuth',
#     'TipDxc1', 'TipDxc2', 'TipDxc3',
#     "RootMxc1", "RootMyc1","RootMzc1",
#     "RootMxc2", "RootMyc2","RootMzc2",
#     "RootMxc3", "RootMyc3","RootMzc3",
#     "RootFzb1", "RootFzb2", "RootFzb3",
#     "RootMxb1", "RootMxb2", "RootMxb3",
#     "RootMyb1", "RootMyb2", "RootMyb3",
#     "Spn1FLzb1", "Spn2FLzb1", "Spn3FLzb1", "Spn4FLzb1", "Spn5FLzb1", "Spn6FLzb1", "Spn7FLzb1", "Spn8FLzb1", "Spn9FLzb1",
#     "Spn1MLxb1", "Spn2MLxb1", "Spn3MLxb1", "Spn4MLxb1", "Spn5MLxb1", "Spn6MLxb1", "Spn7MLxb1", "Spn8MLxb1", "Spn9MLxb1",
#     "Spn1MLyb1", "Spn2MLyb1", "Spn3MLyb1", "Spn4MLyb1", "Spn5MLyb1", "Spn6MLyb1", "Spn7MLyb1", "Spn8MLyb1", "Spn9MLyb1",
#     "Spn1FLzb2", "Spn2FLzb2", "Spn3FLzb2", "Spn4FLzb2", "Spn5FLzb2", "Spn6FLzb2", "Spn7FLzb2", "Spn8FLzb2", "Spn9FLzb2",
#     "Spn1MLxb2", "Spn2MLxb2", "Spn3MLxb2", "Spn4MLxb2", "Spn5MLxb2", "Spn6MLxb2", "Spn7MLxb2", "Spn8MLxb2", "Spn9MLxb2",
#     "Spn1MLyb2", "Spn2MLyb2", "Spn3MLyb2", "Spn4MLyb2", "Spn5MLyb2", "Spn6MLyb2", "Spn7MLyb2", "Spn8MLyb2", "Spn9MLyb2",
#     "Spn1FLzb3", "Spn2FLzb3", "Spn3FLzb3", "Spn4FLzb3", "Spn5FLzb3", "Spn6FLzb3", "Spn7FLzb3", "Spn8FLzb3", "Spn9FLzb3",
#     "Spn1MLxb3", "Spn2MLxb3", "Spn3MLxb3", "Spn4MLxb3", "Spn5MLxb3", "Spn6MLxb3", "Spn7MLxb3", "Spn8MLxb3", "Spn9MLxb3",
#     "Spn1MLyb3", "Spn2MLyb3", "Spn3MLyb3", "Spn4MLyb3", "Spn5MLyb3", "Spn6MLyb3", "Spn7MLyb3", "Spn8MLyb3", "Spn9MLyb3",
#     "TwrBsFxt",  "TwrBsFyt", "TwrBsFzt", "TwrBsMxt",  "TwrBsMyt", "TwrBsMzt",
#     "YawBrFxp", "YawBrFyp", "YawBrFzp", "YawBrMxp", "YawBrMyp", "YawBrMzp",
#     "TwHt1FLxt", "TwHt2FLxt", "TwHt3FLxt", "TwHt4FLxt", "TwHt5FLxt", "TwHt6FLxt", "TwHt7FLxt", "TwHt8FLxt", "TwHt9FLxt",
#     "TwHt1FLyt", "TwHt2FLyt", "TwHt3FLyt", "TwHt4FLyt", "TwHt5FLyt", "TwHt6FLyt", "TwHt7FLyt", "TwHt8FLyt", "TwHt9FLyt",
#     "TwHt1FLzt", "TwHt2FLzt", "TwHt3FLzt", "TwHt4FLzt", "TwHt5FLzt", "TwHt6FLzt", "TwHt7FLzt", "TwHt8FLzt", "TwHt9FLzt",
#     "TwHt1MLxt", "TwHt2MLxt", "TwHt3MLxt", "TwHt4MLxt", "TwHt5MLxt", "TwHt6MLxt", "TwHt7MLxt", "TwHt8MLxt", "TwHt9MLxt",
#     "TwHt1MLyt", "TwHt2MLyt", "TwHt3MLyt", "TwHt4MLyt", "TwHt5MLyt", "TwHt6MLyt", "TwHt7MLyt", "TwHt8MLyt", "TwHt9MLyt",
#     "TwHt1MLzt", "TwHt2MLzt", "TwHt3MLzt", "TwHt4MLzt", "TwHt5MLzt", "TwHt6MLzt", "TwHt7MLzt", "TwHt8MLzt", "TwHt9MLzt",
#     "M1N1FMxe", "M4N1FMxe", "M6N1FMxe", "M8N1FMxe", "M10N1FMxe", "M13N1FMxe", "M15N1FMxe", "M17N1FMxe", "M18N2FMxe",
#     "M1N1FMye", "M4N1FMye", "M6N1FMye", "M8N1FMye", "M10N1FMye", "M13N1FMye", "M15N1FMye", "M17N1FMye", "M18N2FMye",
#     "M1N1FMze", "M4N1FMze", "M6N1FMze", "M8N1FMze", "M10N1FMze", "M13N1FMze", "M15N1FMze", "M17N1FMze", "M18N2FMze",
#     "M1N1MMxe", "M4N1MMxe", "M6N1MMxe", "M8N1MMxe", "M10N1MMxe", "M13N1MMxe", "M15N1MMxe", "M17N1MMxe", "M18N2MMxe",
#     "M1N1MMye", "M4N1MMye", "M6N1MMye", "M8N1MMye", "M10N1MMye", "M13N1MMye", "M15N1MMye", "M17N1MMye", "M18N2MMye",
#     "M1N1MMze", "M4N1MMze", "M6N1MMze", "M8N1MMze", "M10N1MMze", "M13N1MMze", "M15N1MMze", "M17N1MMze", "M18N2MMze",
#     ]


class runFAST_pywrapper(object):

    def __init__(self, **kwargs):

        self.FAST_exe           = None
        self.FAST_lib           = None
        self.FAST_InputFile     = None
        self.FAST_directory     = None
        self.FAST_runDirectory  = None
        self.FAST_namingOut     = None
        self.read_yaml          = False
        self.write_yaml         = False
        self.fst_vt             = {}
        self.case               = {}     # dictionary of variable values to change
        self.channels           = {}     # dictionary of output channels to change
        self.keep_time          = False
        self.use_exe            = False  # use openfast executable instead of library, helpful for debugging sometimes
        self.use_cloud          = False  # use cloud computing
        self.goodman            = False
        self.magnitude_channels = magnitude_channels_default
        self.fatigue_channels   = fatigue_channels_default
        self.la                 = None # Will be initialized on first run through
        self.allow_fails        = False
        self.fail_value         = 9999
        self.write_stdout       = False
        
        self.overwrite_outfiles = True   # True: existing output files will be overwritten, False: if output file with the same name already exists, OpenFAST WILL NOT RUN; This is primarily included for code debugging with OpenFAST in the loop or for specific Optimization Workflows where OpenFAST is to be run periodically instead of for every objective function anaylsis

        self.cloud_machine      = None

        self.cloudConfig        = {}

        # Optional population class attributes from key word arguments
        for (k, w) in kwargs.items():
            try:
                setattr(self, k, w)
            except:
                pass

        super(runFAST_pywrapper, self).__init__()

    def init_crunch(self):
        if self.la is None:
            self.la = LoadsAnalysis(
                outputs=[],
                magnitude_channels=self.magnitude_channels,
                fatigue_channels=self.fatigue_channels,
                #extreme_channels=channel_extremes_default,
            )
        
    def execute(self):

        # FAST version specific initialization
        reader = InputReader_OpenFAST()
        writer = InputWriter_OpenFAST()

        # Read input model, FAST files or Yaml
        if self.fst_vt == {}:
            reader.FAST_InputFile = self.FAST_InputFile
            reader.FAST_directory = self.FAST_directory
            reader.execute()
        
            # Initialize writer variables with input model
            writer.fst_vt = self.fst_vt = reader.fst_vt
        else:
            writer.fst_vt = self.fst_vt
        writer.FAST_runDirectory = self.FAST_runDirectory
        writer.FAST_namingOut = self.FAST_namingOut
        # Make any case specific variable changes
        if self.case:
            writer.update(fst_update=self.case)
        # Modify any specified output channels
        if self.channels:
            writer.update_outlist(self.channels)
        # Write out FAST model
        writer.execute()
        if self.write_yaml:
            writer.FAST_yamlfile = self.FAST_yamlfile_out
            writer.write_yaml()

        # Make sure pCrunch is ready
        self.init_crunch()
            
        if not self.use_exe and not self.use_cloud: # Use library

            FAST_directory = os.path.split(writer.FAST_InputFileOut)[0]
            
            orig_dir = os.getcwd()
            os.chdir(FAST_directory)
        
            openfastlib = FastLibAPI(self.FAST_lib, os.path.abspath(os.path.basename(writer.FAST_InputFileOut)))
            openfastlib.fast_run()

            output_dict = {}
            for i, channel in enumerate(openfastlib.output_channel_names):
                output_dict[channel] = openfastlib.output_values[:,i]
            del(openfastlib)
            
            # Add channel to indicate failed run
            output_dict['openfast_failed'] = np.zeros(len(output_dict[channel]))

            # Calculated channels
            calculate_channels(output_dict, self.fst_vt)

            output = OpenFASTOutput.from_dict(output_dict, self.FAST_namingOut, magnitude_channels=self.magnitude_channels)

            # if save_file: write_fast
            os.chdir(orig_dir)

            if not self.keep_time: output_dict = None

        elif self.use_cloud: # use cloud computing
            wrapper = Inductiva_wrapper()
            writerCloud = copy.deepcopy(writer)

            # we need significant file manipulation to get the cloud to work
            try:
                # Create cloud directory
                cloud_dir = os.path.join(self.FAST_runDirectory, f'cloud_{writerCloud.FAST_namingOut}')
                os.makedirs(cloud_dir, exist_ok=True)  # makedirs is generally preferred over mkdir
                # need to create the Airfoils folder
                # os.makedirs(os.path.join(cloud_dir, "Airfoils"), exist_ok=True)
                
                # Move all matching files
                # source_pattern = os.path.join(self.FAST_runDirectory, f"{writer.FAST_namingOut}*")
                # for file_path in glob.glob(source_pattern):
                #     shutil.move(file_path, cloud_dir)

                # Move the contents in the Airfoil folder
                # source_pattern = os.path.join(self.FAST_runDirectory, "Airfoils", f"{writer.FAST_namingOut}*")
                # for file_path in glob.glob(source_pattern):
                #     shutil.move(file_path, os.path.join(cloud_dir, "Airfoils"))

                # Move the contents from the wind folder
                for wind_file_key in ['FileName_Uni', 'FileName_BTS']:
                    wind_file_path = os.path.join(self.FAST_runDirectory, writer.fst_vt['InflowWind'].get(wind_file_key, ''))
                    if os.path.exists(wind_file_path):
                        shutil.copy(wind_file_path, cloud_dir)
                        writerCloud.fst_vt['InflowWind'][wind_file_key] = os.path.basename(wind_file_path)

                # We need to move the controller file to the new directory
                # This is a bit tricky because the controllers have to be statically linked to avoid library issues
                # within the docker container, approach needs to be refined
                controller_file_path = os.path.join(self.FAST_runDirectory, writer.fst_vt['ServoDyn'].get('DLL_FileName', ''))
                if os.path.exists(controller_file_path):
                    shutil.copy(controller_file_path, cloud_dir)
                    writerCloud.fst_vt['ServoDyn']['DLL_FileName'] = os.path.basename(controller_file_path)

                writerCloud.FAST_runDirectory = cloud_dir
                writerCloud.execute()

            except Exception as e:
                print(f"Error during file operations: {str(e)}")


            # Run FAST
            wrapper.FAST_InputFile = self.FAST_namingOut+'.fst'
            wrapper.FAST_directory = cloud_dir

            wrapper.allow_fails = self.allow_fails
            wrapper.fail_value  = self.fail_value
            wrapper.write_stdout = self.write_stdout

            FAST_Output     = os.path.join(wrapper.FAST_directory, wrapper.FAST_InputFile[:-3]+'outb')
            FAST_Output_txt = os.path.join(wrapper.FAST_directory, wrapper.FAST_InputFile[:-3]+'out')

            #check if OpenFAST is set not to overwrite existing output files, TODO: move this further up in the workflow for minor computation savings
            if True: #self.overwrite_outfiles or (not self.overwrite_outfiles and not (os.path.exists(FAST_Output) or os.path.exists(FAST_Output_txt))):
                failed, task = wrapper.execute(cloudResource = self.cloud_machine)
                if failed:
                    print('OpenFAST Failed! Please check the run logs.')
                    if self.allow_fails:
                        print(f'OpenFAST failures are allowed. All outputs set to {self.fail_value}')
                    else:
                        raise Exception('OpenFAST Failed! Please check the run logs.')
            else:
                failed = False
                print('OpenFAST not executed: Output file "%s" already exists. To overwrite this output file, set "overwrite_outfiles = True".'%FAST_Output)


            # TODO: we need to skip reading the output until after the cloud machine has finished, 
            # so this will be bumped up to the calling function
            '''
            if not failed:
                if os.path.exists(FAST_Output):
                    output_init = OpenFASTBinary(FAST_Output, magnitude_channels=self.magnitude_channels)
                elif os.path.exists(FAST_Output_txt):
                    output_init = OpenFASTAscii(FAST_Output_txt, magnitude_channels=self.magnitude_channels)
                    
                output_init.read()

                # Make output dict
                output_dict = {}
                for i, channel in enumerate(output_init.channels):
                    output_dict[channel] = output_init.df[channel].to_numpy()

                # Add channel to indicate failed run
                output_dict['openfast_failed'] = np.zeros(len(output_dict[channel]))

                # Calculated channels
                calculate_channels(output_dict, self.fst_vt)

                # Re-make output
                output = OpenFASTOutput.from_dict(output_dict, self.FAST_namingOut)
            
            else: # fill with -9999s
                output_dict = {}
                output_dict['Time'] = np.arange(self.fst_vt['Fst']['TStart'],self.fst_vt['Fst']['TMax'],self.fst_vt['Fst']['DT'])
                for module in self.fst_vt['outlist']:
                    for channel in self.fst_vt['outlist'][module]:
                        if self.fst_vt['outlist'][module][channel]:
                            output_dict[channel] = np.full(len(output_dict['Time']),fill_value=self.fail_value, dtype=np.uint8) 

                # Add channel to indicate failed run
                output_dict['openfast_failed'] = np.ones(len(output_dict['Time']), dtype=np.uint8)

                output = OpenFASTOutput.from_dict(output_dict, self.FAST_namingOut, magnitude_channels=self.magnitude_channels)

            # clear dictionary if we're not keeping time
            if not self.keep_time: output_dict = None
            '''

            return task


        else: # use executable
            wrapper = FAST_wrapper()

            # Run FAST
            wrapper.FAST_exe = self.FAST_exe
            wrapper.FAST_InputFile = os.path.split(writer.FAST_InputFileOut)[1]
            wrapper.FAST_directory = os.path.split(writer.FAST_InputFileOut)[0]

            wrapper.allow_fails = self.allow_fails
            wrapper.fail_value  = self.fail_value
            wrapper.write_stdout = self.write_stdout

            FAST_Output     = os.path.join(wrapper.FAST_directory, wrapper.FAST_InputFile[:-3]+'outb')
            FAST_Output_txt = os.path.join(wrapper.FAST_directory, wrapper.FAST_InputFile[:-3]+'out')

            #check if OpenFAST is set not to overwrite existing output files, TODO: move this further up in the workflow for minor computation savings
            if self.overwrite_outfiles or (not self.overwrite_outfiles and not (os.path.exists(FAST_Output) or os.path.exists(FAST_Output_txt))):
                failed = wrapper.execute()
                if failed:
                    print('OpenFAST Failed! Please check the run logs.')
                    if self.allow_fails:
                        print(f'OpenFAST failures are allowed. All outputs set to {self.fail_value}')
                    else:
                        raise Exception('OpenFAST Failed! Please check the run logs.')
            else:
                failed = False
                print('OpenFAST not executed: Output file "%s" already exists. To overwrite this output file, set "overwrite_outfiles = True".'%FAST_Output)

            if not failed:
                if os.path.exists(FAST_Output):
                    output_init = OpenFASTBinary(FAST_Output, magnitude_channels=self.magnitude_channels)
                elif os.path.exists(FAST_Output_txt):
                    output_init = OpenFASTAscii(FAST_Output_txt, magnitude_channels=self.magnitude_channels)
                    
                output_init.read()

                # Make output dict
                output_dict = {}
                for i, channel in enumerate(output_init.channels):
                    output_dict[channel] = output_init.df[channel].to_numpy()

                # Add channel to indicate failed run
                output_dict['openfast_failed'] = np.zeros(len(output_dict[channel]))

                # Calculated channels
                calculate_channels(output_dict, self.fst_vt)

                # Re-make output
                output = OpenFASTOutput.from_dict(output_dict, self.FAST_namingOut)
            
            else: # fill with -9999s
                output_dict = {}
                output_dict['Time'] = np.arange(self.fst_vt['Fst']['TStart'],self.fst_vt['Fst']['TMax'],self.fst_vt['Fst']['DT'])
                for module in self.fst_vt['outlist']:
                    for channel in self.fst_vt['outlist'][module]:
                        if self.fst_vt['outlist'][module][channel]:
                            output_dict[channel] = np.full(len(output_dict['Time']),fill_value=self.fail_value, dtype=np.uint8) 

                # Add channel to indicate failed run
                output_dict['openfast_failed'] = np.ones(len(output_dict['Time']), dtype=np.uint8)

                output = OpenFASTOutput.from_dict(output_dict, self.FAST_namingOut, magnitude_channels=self.magnitude_channels)

            # clear dictionary if we're not keeping time
            if not self.keep_time: output_dict = None



        # Trim Data
        if self.fst_vt['Fst']['TStart'] > 0.0:
            output.trim_data(tmin=self.fst_vt['Fst']['TStart'], tmax=self.fst_vt['Fst']['TMax'])
        case_name, sum_stats, extremes, dels, damage = self.la._process_output(output,
                                                                               return_damage=True,
                                                                               goodman_correction=self.goodman)

        return case_name, sum_stats, extremes, dels, damage, output_dict


class runFAST_pywrapper_batch(object):

    def __init__(self):
        self.FAST_exe           = of_path   # Path to executable
        self.FAST_lib           = lib_path
        self.FAST_InputFile     = None
        self.FAST_directory     = None
        self.FAST_runDirectory  = None

        self.read_yaml          = False
        self.FAST_yamlfile_in   = ''
        self.fst_vt             = {}
        self.write_yaml         = False
        self.FAST_yamlfile_out  = ''

        self.case_list          = []
        self.case_name_list     = []
        self.channels           = {}

        self.overwrite_outfiles = True
        self.keep_time          = False

        self.goodman            = False
        self.magnitude_channels = magnitude_channels_default
        self.fatigue_channels   = fatigue_channels_default
        self.la                 = None
        self.use_exe            = False
        self.use_cloud          = False
        self.allow_fails        = False
        self.fail_value         = 9999
        self.write_stdout       = False
        
        self.post               = None

        self.cloud_machine      = None
        self.cloudConfig        = {}

    def init_crunch(self):
        if self.la is None:
            self.la = LoadsAnalysis(
                outputs=[],
                magnitude_channels=self.magnitude_channels,
                fatigue_channels=self.fatigue_channels,
                #extreme_channels=channel_extremes_default,
            )

    def create_case_data(self):

        case_data_all = []
        for i in range(len(self.case_list)):
            case_data = {}
            case_data['case']               = self.case_list[i]
            case_data['case_name']          = self.case_name_list[i]
            case_data['FAST_exe']           = self.FAST_exe
            case_data['FAST_lib']           = self.FAST_lib
            case_data['FAST_runDirectory']  = self.FAST_runDirectory
            case_data['FAST_InputFile']     = self.FAST_InputFile
            case_data['FAST_directory']     = self.FAST_directory
            case_data['read_yaml']          = self.read_yaml
            case_data['FAST_yamlfile_in']   = self.FAST_yamlfile_in
            case_data['fst_vt']             = self.fst_vt
            case_data['write_yaml']         = self.write_yaml
            case_data['FAST_yamlfile_out']  = self.FAST_yamlfile_out
            case_data['channels']           = self.channels
            case_data['overwrite_outfiles'] = self.overwrite_outfiles
            case_data['use_exe']            = self.use_exe
            case_data['use_cloud']          = self.use_cloud
            case_data['allow_fails']        = self.allow_fails
            case_data['fail_value']         = self.fail_value
            case_data['write_stdout']       = self.write_stdout
            case_data['keep_time']          = self.keep_time
            case_data['goodman']            = self.goodman
            case_data['magnitude_channels'] = self.magnitude_channels
            case_data['fatigue_channels']   = self.fatigue_channels
            case_data['post']               = self.post
            case_data['cloud_machine']      = self.cloud_machine
            case_data['cloudConfig']        = self.cloudConfig


            case_data_all.append(case_data)

        return case_data_all
    
    def run_serial(self):
        # Run batch serially
        if not os.path.exists(self.FAST_runDirectory):
            os.makedirs(self.FAST_runDirectory)

        self.init_crunch()
            
        case_data_all = self.create_case_data()
            
        ss = {}
        et = {}
        dl = {}
        dam = {}
        ct = []
        for c in case_data_all:
            _name, _ss, _et, _dl, _dam, _ct = evaluate(c)
            ss[_name] = _ss
            et[_name] = _et
            dl[_name] = _dl
            dam[_name] = _dam
            ct.append(_ct)
            
        summary_stats, extreme_table, DELs, Damage = self.la.post_process(ss, et, dl, dam)

        return summary_stats, extreme_table, DELs, Damage, ct

    def run_multi(self, cores=None):
        # Run cases in parallel, threaded with multiprocessing module

        if not os.path.exists(self.FAST_runDirectory):
            os.makedirs(self.FAST_runDirectory)

        if not cores:
            cores = mp.cpu_count()
        pool = mp.Pool(cores)

        self.init_crunch()

        case_data_all = self.create_case_data()

        output = pool.map(evaluate_multi, case_data_all)
        pool.close()
        pool.join()

        ss = {}
        et = {}
        dl = {}
        dam = {}
        ct = []
        for _name, _ss, _et, _dl, _dam, _ct in output:
            ss[_name] = _ss
            et[_name] = _et
            dl[_name] = _dl
            dam[_name] = _dam
            ct.append(_ct)
            
        summary_stats, extreme_table, DELs, Damage = self.la.post_process(ss, et, dl, dam)

        return summary_stats, extreme_table, DELs, Damage, ct

    def run_cloud(self, cloudConfig = None):
        # Run in parallel with cloud computing
        import inductiva

        if not os.path.exists(self.FAST_runDirectory):
            os.makedirs(self.FAST_runDirectory)

        self.init_crunch()
        self.use_cloud = True
        self.cloudConfig = cloudConfig

        # Need to allocate the inductiva resources
        self.cloud_machine = inductiva.resources.ElasticMachineGroup( # this needs to be sent over to cloud_wapper to run the job
            machine_type=cloudConfig["machine_type"],
            spot=True,
            min_machines=1,
            max_machines=cloudConfig["max_machines"])
        
        # Also creating the OpenFAST project to allocate runs and collect data, need unique names for the project!!
        folder_name = os.path.basename(os.path.normpath(self.FAST_runDirectory))
        unique_tag = os.urandom(4).hex()
        project_name = f"WEIS_Run_{folder_name}_{unique_tag}"
        openfast_project = inductiva.projects.Project(
                name=project_name,
                append=True)
        
        openfast_project.open()

        case_data_all = self.create_case_data()

        ss = {}
        et = {}
        dl = {}
        dam = {}
        ct = []
        tasks = {}
        for c in case_data_all:
            # we recieve the tasks run on the cloud machine, this makes it easier to download the data back
            _task = evaluate(c)
            # we need to create a task_id to case_name mapping
            tasks[c['case_name']] = _task

        # Once we are done assigning the tasks, we need to wait for all the tasks to finish
        openfast_project.wait()
        openfast_project.close()
        self.cloud_machine.terminate()

        # printing out summary stats
        print(openfast_project)

        # we overide get_output_dir and download the data
        inductiva.get_output_dir = lambda: os.path.join(self.FAST_runDirectory, 'cloud_outputs')

        # # this maynot be advisable if the project name is not unique!!
        # openfast_project.download_outputs()

        # Now we handle trimming and post processing
        for case_name, task in tasks.items():
            
            failed = task.is_failed()
            task.download_outputs()

            if not failed:
                FAST_Output     = os.path.join(self.FAST_runDirectory, 'cloud_outputs', task.id, 'outputs', 
                                           task.FAST_InputFile[:-4]+'.outb')
                FAST_Output_txt = os.path.join(self.FAST_runDirectory, 'cloud_outputs', task.id, 'outputs', 
                                           task.FAST_InputFile[:-4]+'.out')

                if os.path.exists(FAST_Output):
                    output_init = OpenFASTBinary(FAST_Output, magnitude_channels=self.magnitude_channels)
                elif os.path.exists(FAST_Output_txt):
                    output_init = OpenFASTAscii(FAST_Output_txt, magnitude_channels=self.magnitude_channels)
                    
                output_init.read()

                # Make output dict
                output_dict = {}
                for i, channel in enumerate(output_init.channels):
                    output_dict[channel] = output_init.df[channel].to_numpy()

                # Add channel to indicate failed run
                output_dict['openfast_failed'] = np.zeros(len(output_dict[channel]))

                # Calculated channels
                calculate_channels(output_dict, self.fst_vt)

                # Re-make output
                output = OpenFASTOutput.from_dict(output_dict, case_name)
            
            else: # fill with -9999s
                output_dict = {}
                output_dict['Time'] = np.arange(self.fst_vt['Fst']['TStart'],self.fst_vt['Fst']['TMax'],self.fst_vt['Fst']['DT'])
                for module in self.fst_vt['outlist']:
                    for channel in self.fst_vt['outlist'][module]:
                        if self.fst_vt['outlist'][module][channel]:
                            output_dict[channel] = np.full(len(output_dict['Time']),fill_value=self.fail_value, dtype=np.uint8) 

                # Add channel to indicate failed run
                output_dict['openfast_failed'] = np.ones(len(output_dict['Time']), dtype=np.uint8)

                output = OpenFASTOutput.from_dict(output_dict, case_name, magnitude_channels=self.magnitude_channels)

            # Trim Data
            if self.fst_vt['Fst']['TStart'] > 0.0:
                output.trim_data(tmin=self.fst_vt['Fst']['TStart'], tmax=self.fst_vt['Fst']['TMax'])
            case_name, sum_stats, extremes, dels, damage = self.la._process_output(output,
                                                                                return_damage=True,
                                                                                goodman_correction=self.goodman)            

            ss[case_name] = sum_stats
            et[case_name] = extremes
            dl[case_name] = dels
            dam[case_name] = damage
            ct.append(output_dict)
            
        summary_stats, extreme_table, DELs, Damage = self.la.post_process(ss, et, dl, dam)

        return summary_stats, extreme_table, DELs, Damage, ct

    def run_mpi(self, mpi_comm_map_down):

        # Run in parallel with mpi
        from openmdao.utils.mpi import MPI

        # mpi comm management
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        sub_ranks = mpi_comm_map_down[rank]
        size = len(sub_ranks)

        N_cases = len(self.case_list)
        N_loops = int(np.ceil(float(N_cases)/float(size)))
        
        # file management
        if not os.path.exists(self.FAST_runDirectory) and rank == 0:
            os.makedirs(self.FAST_runDirectory)

        self.init_crunch()

        case_data_all = self.create_case_data()

        output = []
        for i in range(N_loops):
            idx_s    = i*size
            idx_e    = min((i+1)*size, N_cases)

            for j, case_data in enumerate(case_data_all[idx_s:idx_e]):
                data   = [evaluate_multi, case_data]
                rank_j = sub_ranks[j]
                comm.send(data, dest=rank_j, tag=0)

            # for rank_j in sub_ranks:
            for j, case_data in enumerate(case_data_all[idx_s:idx_e]):
                rank_j = sub_ranks[j]
                data_out = comm.recv(source=rank_j, tag=1)
                output.append(data_out)

        ss = {}
        et = {}
        dl = {}
        dam = {}
        ct = []
        for _name, _ss, _et, _dl, _dam, _ct in output:
            ss[_name] = _ss
            et[_name] = _et
            dl[_name] = _dl
            dam[_name] = _dam
            ct.append(_ct)

        summary_stats, extreme_table, DELs, Damage = self.la.post_process(ss, et, dl, dam)
        
        return summary_stats, extreme_table, DELs, Damage, ct



def evaluate(indict):
    # Batch FAST pyWrapper call, as a function outside the runFAST_pywrapper_batch class for pickle-ablility

    # Could probably do this with vars(fast), but this gives tighter control
    known_keys = ['case', 'case_name', 'FAST_exe', 'FAST_lib', 'FAST_runDirectory',
                  'FAST_InputFile', 'FAST_directory', 'read_yaml', 'FAST_yamlfile_in', 'fst_vt',
                  'write_yaml', 'FAST_yamlfile_out', 'channels', 'overwrite_outfiles', 'keep_time',
                  'goodman','magnitude_channels','fatigue_channels','post','use_exe','allow_fails','fail_value', 'write_stdout',
                  'use_cloud', 'cloud_machine', 'cloudConfig',
                  ]
    
    fast = runFAST_pywrapper()
    for k in indict:
        if k == 'case_name':
            fast.FAST_namingOut = indict['case_name']
        elif k in known_keys:
            setattr(fast, k, indict[k])
        else:
            print(f'WARNING: Unknown OpenFAST executation parameter, {k}')
    
    return fast.execute()

def evaluate_multi(indict):
    # helper function for running with multiprocessing.Pool.map
    # converts list of arguement values to arguments
    return evaluate(indict)
