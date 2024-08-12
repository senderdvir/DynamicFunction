# Define CountRecords function
def count_records(df, range_between):
    print(len(df))
    if range_between[0] < len(df) < range_between[1]:
        return True
    else:
        return False


def say_hello(name):
    print(name)