"""
TTS route — proxies ElevenLabs so the API key never reaches the browser.
"""
import logging
import os
import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from config import (
    ELEVENLABS_TTS_URL, ELEVENLABS_MODEL_ID,
    ELEVENLABS_SPEED, ELEVENLABS_STABILITY,
    ELEVENLABS_SIMILARITY, ELEVENLABS_STYLE,
    TTS_RATE_LIMIT,
)
from limiter import limiter

logger = logging.getLogger("powerdonor")

router = APIRouter(prefix="/api/tts", tags=["tts"])


class TTSRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=5000)

    model_config = {
        "json_schema_extra": {
            "example": {
                "text": "Hi Sarah, welcome to PowerDonor. Let's find causes that matter to you.",
            }
        }
    }


@router.post(
    "",
    summary="Synthesize speech via ElevenLabs",
    description=(
        "Proxies a text-to-speech request to ElevenLabs and returns an MP3 audio stream. "
        "The ElevenLabs API key is never exposed to the client. "
        "Maximum 5,000 characters of input text. Rate limited to 20 requests/minute per IP."
    ),
    responses={
        429: {"description": "Rate limit exceeded"},
        500: {"description": "ElevenLabs API key not configured"},
        400: {"description": "text is empty or exceeds 5,000 characters"},
    },
)
@limiter.limit(TTS_RATE_LIMIT)
async def synthesize(request: Request, req: TTSRequest):  # `request` required by slowapi for IP extraction
    api_key = os.environ.get("ELEVENLABS_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="ElevenLabs API key not configured")

    payload = {
        "text":     req.text,
        "model_id": ELEVENLABS_MODEL_ID,
        "speed":    ELEVENLABS_SPEED,
        "voice_settings": {
            "stability":        ELEVENLABS_STABILITY,
            "similarity_boost": ELEVENLABS_SIMILARITY,
            "style":            ELEVENLABS_STYLE,
            "use_speaker_boost": True,
        },
    }

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            ELEVENLABS_TTS_URL,
            json=payload,
            headers={
                "xi-api-key": api_key,
                "Accept": "audio/mpeg",
                "Content-Type": "application/json",
            },
            params={"output_format": "mp3_44100_128"},
        )

    if resp.status_code != 200:
        logger.error("ElevenLabs %s: %s", resp.status_code, resp.text[:500])
        raise HTTPException(status_code=resp.status_code, detail=f"TTS service error: {resp.text[:200]}")

    return Response(content=resp.content, media_type="audio/mpeg")
