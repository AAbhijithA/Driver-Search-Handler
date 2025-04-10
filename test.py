import requests

# for i in range(0,5):
#     res = requests.put("http://localhost:3000/updateDriverLoc?userid=124&lat=20.123&lon=27.123&status=O")
#     print(res)

for i in range(0,1):
    res = requests.post("http://localhost:3001/driverSearch?userid=100&lat=20.123&lon=27.122")
    print(res.json())