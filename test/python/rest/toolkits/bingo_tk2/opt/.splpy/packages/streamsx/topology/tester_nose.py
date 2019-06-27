import nose.plugins.base
import os



class JCO(nose.plugins.base.Plugin):
    enabled = False
    name = 'streamsx-jco'
    score = 1000

    def options(self, parser, env=os.environ):
        print('OPTIONS', flush=True)
        super(JCO, self).options(parser, env=env)

    def configure(self, options, conf):
        super(JCO, self).configure(options, conf)
        JCO.enabled = options.enable_plugin_streamsx_jco

    def beforeTest(self, test):
        print('BEFORE', test, type(test.test), flush=True)
        print('BEFORE', test.test, flush=True)
        #print('BEFORE', test.test.test_ctxtype, flush=True)
        test.test.test_jco = {'fred': 3}
