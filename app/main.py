from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.database       import engine, Base
from app.messaging      import connect_rabbitmq, disconnect_rabbitmq
from app.kafka_producer import get_producer, close_producer
from app.routers        import leads, events, audit


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    Base.metadata.create_all(bind=engine)
    print("✓ Database tables ready")
    connect_rabbitmq()
    get_producer()   # initialise Kafka producer
    yield
    # Shutdown
    disconnect_rabbitmq()
    close_producer()


app = FastAPI(
    title="Sales CRM API",
    description="FastAPI backend for the Oracle APEX Sales Pipeline CRM",
    version="3.0.0",
    lifespan=lifespan
)

app.include_router(leads.router)
app.include_router(events.router)
app.include_router(audit.router)


@app.get("/")
def root():
    return {"message": "Sales CRM API is running"}


@app.get("/health")
def health():
    return {"status": "ok"}