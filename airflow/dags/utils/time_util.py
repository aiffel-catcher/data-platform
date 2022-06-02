import pendulum


def get_execution_date(kwargs):
    return kwargs['execution_date'].in_timezone('Asia/Seoul')


def get_last_updated_at(path):
    with open(path) as f:
        last_updated_at= f.read()
        print('get_last_updated_at >>>> ', last_updated_at)
        return last_updated_at


def set_last_updated_at(datetime_str, path):
    dt = pendulum.parse(datetime_str, tz="Asia/Seoul")
    f = open(path, "w")
    print('set_last_updated_at >>>> ', dt.to_iso8601_string())
    f.write(dt.to_iso8601_string())
    f.close()
