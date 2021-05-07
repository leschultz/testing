from foundry import Foundry


def main():
    f = Foundry(no_browser=True, no_local_server=True)

    log = {}
    with open('log.txt', 'r') as infile:
        for line in infile:
            line = line.strip().split(': ')
            log[line[0]] = line[1]

    f.check_status(source_id=log['source_id'])


if __name__ == '__main__':
    main()
