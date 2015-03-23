from kale import worker


if __name__ == '__main__':
    print 'Task worker is running ...'
    worker.Worker().run()
