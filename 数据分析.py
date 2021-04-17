import json
out = open('result2.csv', 'w')
with open('result.json', 'r') as f:
    json_r = json.loads(f.readline().strip())
    print(json_r)
    for item in json_r:
        timestamp = item['timestamp']
        symbol = item['symbol']
        fundingRate = item['fundingRate']
        date = timestamp[:10]
        print(timestamp, fundingRate)
        out.write(','.join([timestamp, str(fundingRate)]) + '\n')
out.close()