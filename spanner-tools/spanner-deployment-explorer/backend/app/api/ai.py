# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Header, status
from pydantic import BaseModel, Field

from app.core.config import get_settings
from app.services.ai_service import AIService

router = APIRouter(prefix="/ai", tags=["AI Assistant (Experimental)"])


class ChatMessage(BaseModel):
    role: str = Field(..., description="'user' or 'assistant'/'model'")
    content: str = Field(..., description="Message text")


class ChatRequest(BaseModel):
    messages: List[ChatMessage] = Field(..., min_length=1, description="Conversation history")
    current_context: Optional[Dict[str, Any]] = Field(default=None, description="Active UI selection state")


class KeyRequest(BaseModel):
    api_key: str = Field(..., min_length=5, description="Google Gemini API key")


class TranscribeRequest(BaseModel):
    audio_base64: str = Field(..., description="Base64 encoded audio recording")
    audio_mime_type: Optional[str] = Field(default="audio/webm", description="MIME type of recorded audio")


class AIStatusResponse(BaseModel):
    enabled: bool
    has_key: bool
    model: str


@router.get("/status", response_model=AIStatusResponse)
async def get_ai_status():
    """Returns AI Assistant feature availability, key presence, and configured model."""
    settings = get_settings(reload=True)
    ai_service = AIService.get_instance()
    has_key = ai_service.has_key()
    return AIStatusResponse(
        enabled=settings.features.enable_ai,
        has_key=has_key,
        model=settings.features.ai_model,
    )


@router.post("/key")
async def save_ai_key(request: KeyRequest):
    """
    Validates the provided Gemini API key against Google Generative Language API,
    and writes it to the untracked gemini.key file upon success.
    """
    ai_service = AIService.get_instance()
    valid = await ai_service.validate_api_key(request.api_key)
    if not valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid Gemini API key. Verification against Google AI Studio failed.",
        )

    ai_service.save_api_key(request.api_key)
    # Refresh settings
    get_settings(reload=True)
    return {
        "status": "active",
        "has_key": True,
        "message": "Gemini API key verified and saved to gemini.key successfully.",
    }


@router.delete("/key")
async def delete_ai_key():
    """Removes the local gemini.key file to deactivate the AI Assistant."""
    ai_service = AIService.get_instance()
    ai_service.delete_api_key()
    get_settings(reload=True)
    return {
        "status": "deactivated",
        "has_key": False,
        "message": "Gemini API key file (gemini.key) removed.",
    }


@router.post("/chat")
async def chat_with_ai(
    request: ChatRequest,
    x_ai_key: Optional[str] = Header(None, alias="X-AI-Key"),
):
    """
    Conducts multi-turn conversation with the Spanner AI Assistant.
    Translates user requirements into benchmark configurations and sizing calculations.
    """
    ai_service = AIService.get_instance()
    if not ai_service.has_key() and not x_ai_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Gemini API key is not configured. Please add gemini.key or provide the key in the UI.",
        )

    raw_messages = [{"role": m.role, "content": m.content} for m in request.messages]
    try:
        response_data = await ai_service.chat(
            messages=raw_messages,
            current_context=request.current_context,
            api_key_override=x_ai_key,
        )
        return response_data
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/transcribe")
async def transcribe_audio_endpoint(
    request: TranscribeRequest,
    x_ai_key: Optional[str] = Header(None, alias="X-AI-Key"),
):
    """
    Transcribes audio using Gemini's audio model with Cloud Spanner technical grounding.
    Returns plain transcribed text to populate directly into the chat input box.
    """
    ai_service = AIService.get_instance()
    if not ai_service.has_key() and not x_ai_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Gemini API key is not configured. Please add gemini.key or provide the key in the UI.",
        )

    try:
        text = await ai_service.transcribe_audio(
            audio_base64=request.audio_base64,
            mime_type=request.audio_mime_type,
            api_key_override=x_ai_key,
        )
        return {"text": text}
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
