"""
Setup Slingshot

Author: kailiu
Date: March 18 2013
"""
import os
import sys

VM = {'prod' : 'slingshot-prod2.build.vmnet.yahoo.com',
      'stage' : 'slingshot-staging.build.vmnet.yahoo.com', 
      'dev' : 'slingshot-dev.build.vmnet.yahoo.com'}

class SlingshotInstaller:
    '''
    Slingshot Installation 
    '''
    def __init__(self, tag, hostname, basepath, env):
        ''' do initialization '''
        self._tag = tag
        self._hostname = hostname
        self._basepath = basepath
        self._env = env
        if self._env not in ('prod', 'stage', 'dev'):
            print 'Unsupported running environment, has to be one of prod, stage, dev'
            sys.exit(-1)

        
    def __del__(self):
        ''' destroy '''
        pass

    def install(self):
        ''' install slingshot packages '''
        print "\033[1m\033[31m \n\t= Slingshot Setup =\n \033[0m\033[22m"

        # Step 1: fetch state file
        print "\033[34m[ Step 1 ]\033[0m fetch the state file..."
        local_file = self.__fetch_state_file()
        
        # Step 2: copy state file to remote gateway/launcher
        print "\033[34m[ Step 2 ]\033[0m copy the state file to remote host..."
        remote_file = self.__copy_to_remote(local_file)

        # Step 3: restore packages on gateway/launcher
        print "\033[34m[ Step 3 ]\033[0m restore packages on remote host..."
        self.__restore_packages(remote_file)

        # Step 4: post installation
        print "\033[34m[ Step 4 ]\033[0m post installation setup..."
        self.__post_install()

        print "\033[1m\033[31m \n\tSlingshot is ready to use now.\n \033[0m\033[22m"


    def __is_available(self, cmd):
        '''
        check to see whether a given cmd is available in current environment. 
        '''
        exitcode = os.system("which %s &>/dev/null" % cmd)
        if exitcode != 0:
            return False
        else:
            return True


    def __fetch_state_file(self):
        ''' fetch the state file based on igor tag '''
        # check to see if igor command is available in local machine
        if not self.__is_available('igor'):
            print "'igor' command is not available in current environment (laptop?), please use another machine (e.g desktop) and try again." 
            sys.exit(-1)

        # fetch state file based on igor tag and running env
        local_state_file = '/tmp/%s.statefile' % self._tag 
        cmd = "igor -tag %s fetch -save %s > %s" % (self._tag, VM[self._env], local_state_file)
        if os.system(cmd) != 0:
            print "Failure in executing '%s'." % cmd
            sys.exit(-1)
        else:
            print "\n... fetching state file is done ...\n"

        # return the file path if it's successful 
        return local_state_file


    def __copy_to_remote(self, local_file):
        ''' copy local file to remote host '''
        # check to see if scp is available in local machine, most likely it should be there. :) 
        if not self.__is_available('scp'):
            print "'scp' command is not available in current environment, are you really running on a linux box?" 
            sys.exit(-1)

        # copy local state file to remote host
        remote_state_file = local_file
        cmd = "scp %s %s:%s" % (local_file, self._hostname, local_file)
        if os.system(cmd) != 0:
            print "Failure in executing '%s'." % cmd
            sys.exit(-1)
        else:
            print "\n... copying state file to remote is done ...\n"

        # return the file path if it's successful 
        return remote_state_file


    def __restore_packages(self, remote_file):
        ''' restore packages based on the state file '''
        cmd = "ssh %s 'yinst restore -yes -nosudo -file %s -root %s'" % (self._hostname, remote_file, self._basepath)
        if os.system(cmd) != 0:
            print "Failure in executing '%s'." % cmd
            sys.exit(-1)
        else:
            print "\n... restoring packages is done ...\n"

    def __post_install(self):
        ''' some post installation steps '''
        cmd = "ssh %s '%s/bin/postinstall.sh'" % (self._hostname, self._basepath)
        if os.system(cmd) != 0:
            print "Failure in executing '%s'." % cmd
            sys.exit(-1)
        else:
            print "\n... post installation is done ...\n"


def main():
    ''' Main fuction of slingshot setup '''
    from optparse import OptionParser
    usage = "usage: this script is to setup slingshot on specified host."
    parser = OptionParser(usage=usage)
    parser.add_option("-t", "--tag",
                      dest="tag", default="",
                      help="the igor tag to be used.")
    parser.add_option("-g", "--hostname",
                      dest="hostname", default="",
                      help="the hostname of either gateway or launcher.")
    parser.add_option("-b", "--basepath",
                      dest="basepath", default="",
                      help="the bash path of installation.")
    parser.add_option("-e", "--env",
                      dest="env", default="stage",
                      help="choose one of prod, stage, dev, default is stage.")
    (options, args) = parser.parse_args()
    
    tag = options.tag
    hostname = options.hostname
    basepath = options.basepath
    env = options.env

    if not tag or not hostname or not basepath:
        print "type -h to see help"
        sys.exit(-1)

    installer = SlingshotInstaller(tag, hostname, basepath, env)
    installer.install()


if __name__ == "__main__":
    main()

