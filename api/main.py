from fastapi import FastAPI

app = FastAPI(title="Data Warehouse Música")

@app.get("/")
def root():
    return {"message": "¡API del Data Warehouse lista!"}
