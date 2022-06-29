import argparse



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("echo", help="echo the positional argument string")
    parser.add_argument("--square", nargs='+',help="square the positional argument echo")
    parser.add_argument("-f", help = "file name")
    args = parser.parse_args()

    print(f"args = {args}")
    print(f"args.echo = {args.echo}")
    if args.square is None:
        print("args.square is None")
    else:
        print(f"args.square = {args.square=}")
    print(f"args.f = {args.f}")
