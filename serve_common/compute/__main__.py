import sys
import pickle


def main():
    target, *args = pickle.load(sys.stdin.buffer)
    target(*args)

if __name__ == '__main__':
    main()
