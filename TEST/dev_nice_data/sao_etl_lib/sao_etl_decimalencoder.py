import decimal, json, datetime

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        elif isinstance(o, datetime.date):
            return o.strftime('%Y-%m-%d %H:%M:%S')
        return super(DecimalEncoder, self).default(o)