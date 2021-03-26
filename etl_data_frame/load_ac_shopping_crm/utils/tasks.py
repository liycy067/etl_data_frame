import argparse


class BaseTask:

    def __init__(self,task_name):
        self.task_status = 'Ready'
        self._initialize()

    def _initialize(self):
        try:
            self.task_status = 'Initialize'
            parser = argparse.ArgumentParser(description='Process Arguments for Tasks')
            self.args(parser)
            args = parser.parse_args()
            self.configure(args)
        except Exception as e:
            raise e

    def args(self, parser):
        pass

    def configure(self, args):
        pass
    
    def main(self):
        pass

    def run(self):
        try:
            self.task_status = 'In Progress'
            self.main()
            self.task_status = 'Successed'
        except Exception as e:
            self.task_status = 'Failed'
            raise e
    def __str__(self):
        return self.task_status 
