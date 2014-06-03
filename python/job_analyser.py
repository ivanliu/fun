"""
Analyser for hadoop job

Author: kailiu
"""
import re
import logging
from urlparse import urljoin

from sweeper import Sweeper
from utils.config import Config
from utils.BeautifulSoup import BeautifulSoup
from utils.extractor import Extractor

class Analyser:
    '''
    job analyser
    '''
    def __init__(self, jobid, grid, jobuser=''):
        ''' do initialization '''
        # init config
        conf = Config('analyser.conf')
        user = conf.account.user
        passwd = conf.account.passwd
        self.jobid = jobid
        self.history_link = conf.history_links[grid] % jobuser
        logging.info("history link: %s" % self.history_link)

        # links
        self.joblink = ''
        self.maplink = ''
        self.reducelink = ''

        # data
        self.ginfo = {}
        self.runtimes = {}

        # prepare 
        self.sweeper = Sweeper(user, passwd, "socks.yahoo.com:1080") 

    def __del__(self):
        ''' destroy '''
        pass

    def analyse(self):
        ''' analyse the job '''
        # parse history page
        self.__parse_history_page()
        if not self.joblink:
            logging.error("Failed to get job link.")
            return False

        # parse job page
        self.__parse_job_page()
        if not self.maplink or not self.reducelink:
            logging.error("Failed to get map/reduce link.")
            return False

        # parse task pages
        self.__parse_task_pages('map')
        self.__parse_task_pages('reduce')
        

    def __parse_history_page(self):
        ''' extract job link from job history by jobid '''
        # get page
        page = self.sweeper.getpage(self.history_link)
        if not page:
            return None

        # debug
        fd = open('history.html', 'w')
        fd.write(page)
        fd.close()

        # parse by jobid
        soup = BeautifulSoup(page)
        text_nodes = soup.findAll(text=self.jobid) 
        if len(text_nodes) < 1:
            logging.error("Failed to parse job link from history viewer for jobid [%s]." % self.jobid)
        elif len(text_nodes) > 1:
            logging.error("Ooops, more than one links found in history viewer for jobid [%s]." % self.jobid)
        else:
            anchor = text_nodes[0].parent
            href = anchor.get('href')
            if href:
                self.joblink = urljoin(self.history_link, str(href)) 

    def __parse_job_page(self):
        ''' extract global job info, map/reduce links '''
        # get page
        page = self.sweeper.getpage(self.joblink)
        if not page:
            return None

        # debug
        fd = open('job.html', 'w')
        fd.write(page)
        fd.close()

        soup = BeautifulSoup(page)

        # parse jobname
        text_nodes = soup.findAll(text=re.compile('JobName:')) 
        if len(text_nodes) == 1:
            jobname = text_nodes[0].parent.nextSibling
            if jobname:
                self.ginfo['jobname'] = str(jobname).strip()

        # parse status
        text_nodes = soup.findAll(text=re.compile('Status:')) 
        if len(text_nodes) == 1:
            status = text_nodes[0].parent.nextSibling
            if status:
                self.ginfo['status'] = str(status).strip()

        # parse runtime
        text_nodes = soup.findAll(text=re.compile('Finished At:')) 
        if len(text_nodes) == 1:
            runtime = text_nodes[0].parent.nextSibling
            if runtime:
                runtime = str(runtime).strip()
                self.ginfo['runtime'] = runtime[runtime.find('(')+1 : runtime.find(')')]

        # parse map/reduce page links
        tables = soup.findAll('table')
        if len(tables) > 0:
            table = tables[0]
            maps = table.findAll(text="Map")
            if len(maps) == 1:
                map = maps[0]
                maplink = map.parent.parent.findAll('td')[2].find('a').get('href')
                if maplink:
                    self.maplink = urljoin(self.history_link, str(maplink).strip())
            reduces = table.findAll(text="Reduce")
            if len(reduces) == 1:
                reduce = reduces[0]
                reducelink = reduce.parent.parent.findAll('td')[2].find('a').get('href')
                if reducelink:
                    self.reducelink = urljoin(self.history_link, str(reducelink).strip())

    def __parse_task_pages(self, tasktype):
        ''' extract runtimes '''
        # get page
        page = ''
        if tasktype == 'map':
            page = self.sweeper.getpage(self.maplink)
        elif tasktype =='reduce':
            page = self.sweeper.getpage(self.reducelink)
        if not page:
            return None

        # debug
        if tasktype == 'map':
            outfile = 'map.html'
        elif tasktype == 'reduce':
            outfile = 'reduce.html'
        fd = open(outfile, 'w')
        fd.write(page)
        fd.close()

        # extract runtimes
        extractor = Extractor()
        extractor.feed(page)
        self.runtimes[tasktype] = extractor.get_runtimes()

    def report(self, output):
        ''' analysis reporting '''
        print "========== job info =========="
        print "Name:\t\t", self.ginfo['jobname']
        print "Status:\t\t", self.ginfo['status']
        print "Runtime:\t", self.ginfo['runtime']

        # output runtimes
        map_output = "%s.map" % output
        red_output = "%s.red" % output
        fmap = open(map_output, 'w')
        fmap.write("%s\n" % ('\n'.join([ str(x) for x in self.runtimes['map'] ])))
        fmap.close()
        fred = open(red_output, 'w')
        fred.write("%s\n" % ('\n'.join([ str(x) for x in self.runtimes['reduce'] ])))
        fred.close()


def main():
    ''' Main fuction of analyser module '''
    from optparse import OptionParser
    usage = "usage: this tool is an analyser for hadoop job."
    parser = OptionParser(usage=usage)
    parser.add_option("-j", "--jobid",
                      dest="jobid", default="",
                      help="jobid is something like: job_201010180834_405775")
    parser.add_option("-g", "--grid",
                      dest="grid", default="",
                      help="which grid is your job run on, supporting list: dg, db, mg, mb")
    parser.add_option("-u", "--user",
                      dest="user", default="",
                      help="which user is your job run with")
    parser.add_option("-o", "--output",
                      dest="output", default="",
                      help="save the report into output")
    (options, args) = parser.parse_args()
    
    jobid = options.jobid
    grid = options.grid
    user = options.user
    output = options.output

    if not jobid or not grid or not output:
        print "type -h to see help"
        exit(-1)

    analyser = Analyser(jobid, grid, user)
    analyser.analyse()
    analyser.report(output)


if __name__ == "__main__":
    main()

