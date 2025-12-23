from fastapi import FastAPI, HTTPException
import httpx

app =  FastAPI()

SLEEPER_BASE_URL = "https://api.sleeper.app/v1"

async def sleeper_get(path: str, params: dict | None = None):
    url = f"{SLEEPER_BASE_URL}{path}"

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
    except httpx.RequestError as e:
        # Network/DNS/connection problem
        raise HTTPException(status_code=502, detail=f"Could not reach Sleeper: {e}")

    if resp.status_code >= 400:
        # Sleeper returned an error (404, 500, etc)
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return resp.json()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/sleeper/user/{user_id}")
async def get_user(user_id: str):
    return await sleeper_get(f"/user/{user_id}")

@app.get("/sleeper/{user_id}/leagues/{sport}/{season}")
async def get_user_leagues(user_id: str, season: str, sport: str):
    return await sleeper_get(f"/user/{user_id}/leagues/{sport}/{season}")

@app.get("/sleeper/league/{league_id}")
async def get_league(league_id: str):
    return await sleeper_get(f"/league/{league_id}")

@app.get("/sleeper/league/{league_id}/rosters")
async def get_league(league_id: str):
    return await sleeper_get(f"/league/{league_id}/rosters")