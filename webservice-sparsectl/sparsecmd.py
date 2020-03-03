#!/usr/bin/env python

# Use an sfused config file to generate a proper sparsectl command,
# and then run that command with the provided args.
#
# Examples:
#
# $ sparsecmd.py -c /tmp/dir_async0_glu1.json -- create 12345
# $ sparsecmd.py -c /tmp/dir_async0_glu1.json -- dump 12345
# $ sparsecmd.py -c /tmp/dir_async0_glu1.json -- stat 12345
# $ sparsecmd.py -c /tmp/dir_async0_glu1.json -- delete 12345
#

import json, re, sys, argparse, os

def err(msg):
    print msg
    sys.exit(1)

def section_num(s):
    return int(s.split(':')[1])

def sections_by_num(conf, pattern):
    return dict((section_num(s), conf[s]) for s in conf.keys() if re.match(pattern, s))

def driver_conf(driver, compat=False):
    driver_prefix = '' if compat else 'ring_driver.'
    return ';'.join(driver_prefix + '%s=%s'%(k, str(v)) for (k,v) in driver.items() )

def gen_sparse_opts(confpath, sparse_ino_mode=None, compat=False):
    with open(confpath) as f:
        s = ''.join(re.sub('//.*', '', l) for l in f) # remove comments after //...
    conf = json.loads(s)

    drivers   = sections_by_num(conf, 'ring_driver')
    caches    = sections_by_num(conf, 'cache')
    ino_modes = sections_by_num(conf, 'ino_mode')

    sparse_section_nums = [n for (n,sect) in ino_modes.items() if sect['type'] == 'sparse']
    if len(sparse_section_nums) == 0:
        err("config file '%s' does not contain any ino mode"
            " of type 'sparse'" % confpath)

    if sparse_ino_mode is None:
        if len(sparse_section_nums) != 1:
            err("config file '%s' contains several ino modes"
                " of type 'sparse', please select one of: %s" \
                    % (confpath,
                       ' '.join(map(str, sparse_section_nums))))
        sparse_section = ino_modes[sparse_section_nums[0]]
    else:
        if not sparse_ino_mode in sparse_section_nums:
            err("config file '%s' ino mode %d is not"
                " of type 'sparse', please select one of: %s" \
                    % (confpath,
                       sparse_ino_mode,
                       ' '.join(map(str, sparse_section_nums))))
        sparse_section = ino_modes[sparse_ino_mode]

    def cache_num_to_driver_section(n):
        return drivers[ caches[n]['ring_driver'] ]

    driver_st = cache_num_to_driver_section(sparse_section['cache_stripes'])
    driver_md = cache_num_to_driver_section(sparse_section['cache_md'])
    if ('cache_main' in sparse_section
        and sparse_section['cache_main'] >=0):
        driver_main = cache_num_to_driver_section(sparse_section['cache_main'])
    else:
        driver_main = driver_md

    sparse_opts = ['-S']
    sparse_opts += ['-b', driver_conf(driver_st, compat)]
    sparse_opts += ['-B', driver_conf(driver_md, compat)]
    sparse_opts += ['-M', driver_conf(driver_main, compat)]

    opt_map = {
        'fsid':         '-f',
        'main_cos':     '-c',
        'page_cos':     '-p',
        'stripe_cos':   '-C',
        'stripe_size':  '-s',
    }

    for (k, opt) in opt_map.items():
        if k in sparse_section:
            sparse_opts += [opt, str(sparse_section[k])]

    return sparse_opts

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--conf', '-c',
                        required=True,
                        help='sfused config file')
    parser.add_argument('--ino_mode', '-i',
                        type=int,
                        help='ino_mode (option is needed when there are several)')
    parser.add_argument('--compat', '-C',
                        action='store_true',
                        help='enable compatibility with older sparsectl syntax')
    parser.add_argument('--verbose', '-v',
                        action='store_true',
                        help='show command before running it')
    parser.add_argument('--dry_run', '-n',
                        action='store_true',
                        help='do not run the command')
    parser.add_argument('args',
                        metavar='A', nargs='*',
                        help='extra args')
    args = parser.parse_args(sys.argv[1:])

    cmd = ['/home/website/bin/sparsectl'] + gen_sparse_opts(args.conf, args.ino_mode, args.compat) + args.args

    if args.verbose:
        print 'command:' + ' '.join(map(lambda s: "'%s'" % s, cmd))
    if args.dry_run:
        ret = 0
    else:
        ret = os.spawnvp(os.P_WAIT, cmd[0], cmd)
    sys.exit(ret)

if __name__ == '__main__':
    main()
