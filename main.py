import os
import uvicorn
from api import create_app

csv_path = os.getenv("CSV_PATH", "/Users/ugopaulduraffourd/Library/CloudStorage/GoogleDrive-ugopaul.duraffourd.pro@gmail.com/Mon Drive/HSS_OpenAPI_Dynamic/kwh_hourly_feb25.csv")
app = create_app(csv_path=csv_path)

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "4010"))
    
    uvicorn.run("main:app", host=host, port=port, reload=True)