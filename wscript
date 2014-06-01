def options(opt):
        opt.load('compiler_c compiler_cxx')
def configure(cnf):
        cnf.load('compiler_c compiler_cxx')
        cnf.check(features='cxx cxxprogram', cxxflags=['-std=c++11', '-Wall'])
def build(bld):
        bld(features='cxx cxxprogram', source='src/kpp_protocol.cpp', cxxflags=['-std=c++11', '-Wall', '-I../src'], target='libkpp')
