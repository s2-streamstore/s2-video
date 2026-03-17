// S2 Video Conference — Frontend

const MSG_VIDEO = 0x01;
const MSG_AUDIO = 0x02;
const MSG_CONTROL = 0x03;
const MSG_CHAT = 0x04;
const MSG_SCREEN = 0x05;
const MSG_STATS = 0x06;

let ws = null;
let localStream = null;
let captureInterval = null;
let audioContext = null;
let audioWorklet = null;
let localUserID = null;
let cameraEnabled = true;
let micEnabled = true;
let handRaised = false;
let screenStream = null;
let screenCaptureInterval = null;
let videoEncodeInFlight = false;
let screenEncodeInFlight = false;
let lastVideoCaptureAt = 0;

let replayRoom = null;
let replayUser = null;
let replaySpeed = 1;
let replayFocus = ''; // Empty = all participants, or a specific user ID.
let replayTimeline = null;
let replayCurrentTs = 0;
let replayTimerInterval = null;
let replayControlsBound = false;
let replayPaused = false;
let replayLoading = false;

// Remote participant canvases and audio players.
const remoteVideos = new Map();
const remoteAudio = new Map();

// Check URL params for room code (shareable link).
(function checkShareableLink() {
  const params = new URLSearchParams(location.search);
  const room = params.get('room');
  if (room) {
    const roomInput = document.getElementById('room-input');
    if (roomInput) roomInput.value = room;
  }
})();

document.getElementById('create-btn').addEventListener('click', createInstantMeeting);

document.getElementById('join-btn').addEventListener('click', () => {
  const room = document.getElementById('room-input').value.trim();
  const user = document.getElementById('user-input').value.trim();
  if (!room || !user) return alert('Enter room code and name');
  joinRoom(room, user, false);
});

document.getElementById('replay-btn').addEventListener('click', () => {
  const room = (document.getElementById('replay-room-input') || document.getElementById('room-input')).value.trim();
  const user = document.getElementById('user-input').value.trim() || 'viewer';
  if (!room) return alert('Enter a room code to replay');
  joinRoom(room, user, true);
});

async function createInstantMeeting() {
  const user = document.getElementById('user-input').value.trim();
  if (!user) return alert('Enter your name first');

  const btn = document.getElementById('create-btn');
  btn.disabled = true;
  btn.querySelector('span').textContent = 'Creating...';

  try {
    const resp = await fetch('/api/rooms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: user }),
    });
    const data = await resp.json();

    const shareUrl = `${location.origin}?room=${encodeURIComponent(data.id)}`;

    document.getElementById('share-link').value = shareUrl;
    document.getElementById('share-modal').classList.remove('hidden');
    document.getElementById('room-input').value = data.id;

    document.getElementById('join-created-btn').onclick = () => {
      history.replaceState(null, '', `?room=${encodeURIComponent(data.id)}`);
      joinRoom(data.id, user, false);
    };
  } catch (e) {
    alert('Failed to create room: ' + e.message);
  } finally {
    btn.disabled = false;
    btn.querySelector('span').textContent = 'Create instant meeting';
  }
}

function copyShareLink() {
  const input = document.getElementById('share-link');
  navigator.clipboard.writeText(input.value);
  const btn = document.getElementById('copy-link-btn');
  btn.textContent = 'Copied!';
  setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
}

document.getElementById('leave-btn').addEventListener('click', leaveRoom);
document.getElementById('replay-leave-btn').addEventListener('click', leaveRoom);
document.getElementById('toggle-camera').addEventListener('click', toggleCamera);
document.getElementById('toggle-mic').addEventListener('click', toggleMic);
document.getElementById('toggle-hand').addEventListener('click', toggleHand);

async function joinRoom(room, user, replay) {
  if (replay) {
    document.getElementById('landing').classList.add('hidden');
    document.getElementById('replay-page').classList.remove('hidden');
    document.getElementById('replay-title').textContent = `Replay: ${room}`;
    replayRoom = room;
    replayUser = user;
    replaySpeed = 1;
    localUserID = user;
    await initReplay(room, user);
    return;
  }

  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      video: { width: 640, height: 480, frameRate: 30 },
      audio: true,
    });
  } catch (e) {
    localStream = null;
  }

  document.getElementById('landing').classList.add('hidden');
  document.getElementById('room-page').classList.remove('hidden');
  document.getElementById('room-title').textContent = room;

  const localVideo = document.getElementById('local-video');
  if (localStream) {
    localVideo.srcObject = localStream;
  } else {
    // Camera/mic unavailable — mark local tile so everyone can see it's off.
    cameraEnabled = false;
    micEnabled = false;
    document.getElementById('local-tile').classList.add('camera-off');
    document.getElementById('toggle-camera').classList.add('off');
    document.getElementById('toggle-mic').classList.add('off');
    document.getElementById('room-status').textContent = 'No camera/mic — joining as viewer';
  }

  localUserID = user;
  connectWebSocket(room, user, false, 0);
}

function closeCurrentSocket() {
  if (!ws) return;
  const socket = ws;
  ws = null;
  socket.close();
}

function setReplayStatus(text) {
  const statusEl = document.getElementById('replay-status');
  if (statusEl) statusEl.textContent = text;
}

function updateReplayPlayButton() {
  const btn = document.getElementById('replay-play-btn');
  if (!btn) return;
  btn.textContent = replayPaused ? 'Play' : 'Pause';
}

function markReplayLoaded() {
  if (!replayLoading || replayPaused) return;
  replayLoading = false;
  setReplayStatus('Playing');
}

function resetReplayChatState() {
  const container = document.getElementById('chat-messages');
  if (container) container.innerHTML = '';
  const toggle = document.getElementById('toggle-chat');
  if (toggle) toggle.classList.remove('has-unread');
}

function clearReplayHandIndicators() {
  document.querySelectorAll('#replay-grid .hand-indicator').forEach((node) => node.remove());
  document.querySelectorAll('#replay-grid .video-tile.speaking').forEach((tile) => tile.classList.remove('speaking'));
}

function resetReplayAudioState() {
  remoteAudio.clear();
  document.querySelectorAll('#replay-grid .video-tile.speaking').forEach((tile) => tile.classList.remove('speaking'));
  if (sharedPlaybackCtx) {
    const ctx = sharedPlaybackCtx;
    sharedPlaybackCtx = null;
    ctx.close().catch(() => {});
  }
}

function leaveRoom() {
  closeCurrentSocket();
  if (captureInterval) clearInterval(captureInterval);
  captureInterval = null;
  if (localStream) {
    localStream.getTracks().forEach(t => t.stop());
    localStream = null;
  }
  if (audioContext) {
    audioContext.close();
    audioContext = null;
  }
  if (sharedPlaybackCtx) {
    sharedPlaybackCtx.close();
    sharedPlaybackCtx = null;
  }

  stopScreenShare();
  if (replayTimerInterval) { clearInterval(replayTimerInterval); replayTimerInterval = null; }
  replayTimeline = null;
  replayPaused = false;
  replayLoading = false;
  videoEncodeInFlight = false;
  screenEncodeInFlight = false;
  lastVideoCaptureAt = 0;
  cameraEnabled = true;
  micEnabled = true;
  handRaised = false;
  document.getElementById('toggle-camera').classList.remove('off');
  document.getElementById('toggle-mic').classList.remove('off');
  document.getElementById('toggle-hand').classList.remove('hand-active');

  remoteVideos.clear();
  remoteAudio.clear();
  remoteScreens.clear();
  resetReplayChatState();
  clearReplayHandIndicators();

  document.getElementById('room-page').classList.add('hidden');
  document.getElementById('replay-page').classList.add('hidden');
  document.getElementById('landing').classList.remove('hidden');
  document.getElementById('video-grid').innerHTML = `
    <div class="video-tile" id="local-tile">
      <video id="local-video" autoplay muted playsinline></video>
      <canvas id="local-canvas" class="hidden"></canvas>
      <div class="video-label">You</div>
    </div>`;
  document.getElementById('replay-grid').innerHTML = '';
}

function connectWebSocket(room, user, replay, from, speed) {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  let url = `${proto}//${location.host}/ws?room=${encodeURIComponent(room)}&user=${encodeURIComponent(user)}`;
  if (replay) {
    url += `&replay=true&from=${from}`;
    if (speed && speed !== 1) url += `&speed=${speed}`;
    if (replayFocus) url += `&focus=${encodeURIComponent(replayFocus)}`;
  }

  const socket = new WebSocket(url);
  socket.binaryType = 'arraybuffer';
  ws = socket;

  socket.onopen = () => {
    if (socket !== ws) return;

    const statusEl = replay
      ? document.getElementById('replay-status')
      : document.getElementById('room-status');

    if (replay) {
      statusEl.textContent = replayLoading ? 'Loading replay…' : (replayPaused ? 'Paused' : 'Connected');
      return;
    }

    statusEl.textContent = 'Connected';
    startVideoCapture();
    startAudioCapture();
  };

  socket.onmessage = (event) => {
    if (socket !== ws) return;

    const data = new Uint8Array(event.data);
    if (data.length < 1) return;

    const type = data[0];

    if (type === MSG_CONTROL) {
      const json = new TextDecoder().decode(data.slice(1));
      const msg = JSON.parse(json);
      if (replay && msg.type !== 'replay_time') markReplayLoaded();
      handleControl(msg, replay);
      return;
    }

    if (type === MSG_STATS) {
      const json = new TextDecoder().decode(data.slice(1));
      updateMetricsOverlay(JSON.parse(json));
      return;
    }

    if (type === MSG_CHAT) {
      if (replay) markReplayLoaded();
      const json = new TextDecoder().decode(data.slice(1));
      handleChatMessage(JSON.parse(json));
      return;
    }

    if (type === MSG_VIDEO || type === MSG_AUDIO || type === MSG_SCREEN) {
      if (data.length < 3) return;
      const userIdLen = (data[1] << 8) | data[2];
      if (data.length < 3 + userIdLen) return;
      const userId = new TextDecoder().decode(data.slice(3, 3 + userIdLen));
      const payload = data.slice(3 + userIdLen);

      if (replay) markReplayLoaded();
      if (type === MSG_VIDEO) {
        renderRemoteVideo(userId, payload, replay);
      } else if (type === MSG_SCREEN) {
        renderScreenShare(userId, payload, replay);
      } else {
        playRemoteAudio(userId, payload);
      }
    }
  };

  socket.onclose = () => {
    if (socket !== ws) return;

    const statusEl = replay
      ? document.getElementById('replay-status')
      : document.getElementById('room-status');

    if (replay) {
      statusEl.textContent = 'Disconnected';
      return;
    }

    statusEl.textContent = 'Disconnected';
  };
}

function handleControl(msg, replay) {
  const grid = replay
    ? document.getElementById('replay-grid')
    : document.getElementById('video-grid');

  switch (msg.type) {
    case 'participants':
      updateParticipantTiles(msg.participants || [], grid, replay);
      if (replay && typeof msg.timestamp === 'number' && msg.timestamp > 0) {
        replayCurrentTs = msg.timestamp;
        updateReplayProgress(replayCurrentTs);
      }
      break;
    case 'join':
      if (replay && typeof msg.timestamp === 'number' && msg.timestamp > 0) {
        replayCurrentTs = msg.timestamp;
        updateReplayProgress(replayCurrentTs);
      }
      if (msg.user_id) {
        ensureParticipantTile(msg.user_id, grid);
      }
      break;
    case 'leave':
      if (replay && typeof msg.timestamp === 'number' && msg.timestamp > 0) {
        replayCurrentTs = msg.timestamp;
        updateReplayProgress(replayCurrentTs);
      }
      removeParticipantTile(msg.user_id, grid);
      break;
    case 'replay_time':
      if (typeof msg.timestamp === 'number' && msg.timestamp > 0) {
        replayCurrentTs = msg.timestamp;
        updateReplayProgress(replayCurrentTs);
      }
      break;
    case 'replay_complete':
      replayPaused = true;
      replayLoading = false;
      updateReplayPlayButton();
      setReplayStatus('Replay complete');
      if (typeof msg.timestamp === 'number' && msg.timestamp > 0) {
        replayCurrentTs = msg.timestamp;
        updateReplayProgress(replayCurrentTs);
      } else {
        updateReplayProgress(replayTimeline ? replayTimeline.end : 0);
      }
      if (replayTimerInterval) {
        clearInterval(replayTimerInterval);
        replayTimerInterval = null;
      }
      break;
    case 'screen_stop':
      if (msg.user_id) {
        const screenTile = document.getElementById(`screen-${msg.user_id}`);
        if (screenTile) screenTile.remove();
        remoteScreens.delete(msg.user_id);
        updateTileCount();
      }
      break;
    case 'hand':
      if (replay && typeof msg.timestamp === 'number' && msg.timestamp > 0) {
        replayCurrentTs = msg.timestamp;
        updateReplayProgress(replayCurrentTs);
      }
      if (msg.user_id) {
        const tid = `tile-${msg.user_id}`;
        setHandIndicator(tid, msg.raised);
      }
      break;
    case 'error':
      alert(msg.message);
      break;
  }
}

function updateParticipantTiles(participants, grid, replay) {
  const activeSet = new Set(participants);

  // Add new tiles.
  for (const uid of participants) {
    if (!replay && uid === localUserID) continue;
    ensureParticipantTile(uid, grid);
  }

  // Remove tiles for users who left.
  for (const uid of Array.from(remoteVideos.keys())) {
    if (!activeSet.has(uid)) {
      removeParticipantTile(uid, grid);
    }
  }
  for (const uid of Array.from(remoteScreens.keys())) {
    if (!activeSet.has(uid)) {
      removeParticipantTile(uid, grid);
    }
  }
}

function updateTileCount() {
  const grid = document.getElementById('video-grid');
  if (grid) grid.dataset.tiles = grid.querySelectorAll('.video-tile').length;
  const rgrid = document.getElementById('replay-grid');
  if (rgrid) rgrid.dataset.tiles = rgrid.querySelectorAll('.video-tile').length;
}

function ensureParticipantTile(userId, grid) {
  if (remoteVideos.has(userId)) return;

  const tile = document.createElement('div');
  tile.className = 'video-tile';
  tile.id = `tile-${userId}`;

  const canvas = document.createElement('canvas');
  canvas.width = 640;
  canvas.height = 480;
  tile.appendChild(canvas);

  const label = document.createElement('div');
  label.className = 'video-label';
  label.textContent = userId;
  tile.appendChild(label);

  grid.appendChild(tile);
  remoteVideos.set(userId, {
    canvas,
    ctx: canvas.getContext('2d'),
    pendingFrame: null,
    renderScheduled: false,
    decoding: false,
  });
  updateTileCount();
}

function removeParticipantTile(userId, grid) {
  const tile = document.getElementById(`tile-${userId}`);
  if (tile) tile.remove();
  const screenTile = document.getElementById(`screen-${userId}`);
  if (screenTile) screenTile.remove();
  remoteVideos.delete(userId);
  remoteAudio.delete(userId);
  remoteScreens.delete(userId);
  updateTileCount();
}

function toggleCamera() {
  cameraEnabled = !cameraEnabled;
  const btn = document.getElementById('toggle-camera');
  btn.classList.toggle('off', !cameraEnabled);

  if (localStream) {
    localStream.getVideoTracks().forEach(t => t.enabled = cameraEnabled);
  }

  const localVideo = document.getElementById('local-video');
  const localTile = document.getElementById('local-tile');
  if (!cameraEnabled) {
    localVideo.style.opacity = '0';
    localTile.classList.add('camera-off');
  } else {
    localVideo.style.opacity = '1';
    localTile.classList.remove('camera-off');
  }
}

function toggleMic() {
  micEnabled = !micEnabled;
  const btn = document.getElementById('toggle-mic');
  btn.classList.toggle('off', !micEnabled);

  if (localStream) {
    localStream.getAudioTracks().forEach(t => t.enabled = micEnabled);
  }
}

function toggleHand() {
  handRaised = !handRaised;
  const btn = document.getElementById('toggle-hand');
  btn.classList.toggle('hand-active', handRaised);

  // Show/hide on local tile.
  setHandIndicator('local-tile', handRaised);

  // Send to others via a control message over WS.
  if (ws && ws.readyState === WebSocket.OPEN) {
    const ctrl = JSON.stringify({ type: 'hand', user_id: localUserID, raised: handRaised });
    const bytes = new TextEncoder().encode(ctrl);
    const msg = new Uint8Array(1 + bytes.length);
    msg[0] = MSG_CONTROL;
    msg.set(bytes, 1);
    ws.send(msg);
  }
}

function setHandIndicator(tileId, raised) {
  const tile = document.getElementById(tileId);
  if (!tile) return;
  let indicator = tile.querySelector('.hand-indicator');
  if (raised) {
    if (!indicator) {
      indicator = document.createElement('div');
      indicator.className = 'hand-indicator';
      indicator.textContent = '\u270B';
      tile.appendChild(indicator);
    }
  } else if (indicator) {
    indicator.remove();
  }
}

// Detect WebP support for ~30% smaller frames than JPEG.
const supportsWebP = (() => {
  const c = document.createElement('canvas');
  c.width = 1; c.height = 1;
  return c.toDataURL('image/webp').startsWith('data:image/webp');
})();
const imageFormat = supportsWebP ? 'image/webp' : 'image/jpeg';

function startVideoCapture() {
  const video = document.getElementById('local-video');
  const canvas = document.getElementById('local-canvas');
  canvas.width = 640;
  canvas.height = 480;
  const ctx = canvas.getContext('2d');

  captureInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (!cameraEnabled) return;
    if (video.readyState < video.HAVE_CURRENT_DATA) return;
    if (videoEncodeInFlight) return;

    const profile = getVideoCaptureProfile();
    const now = performance.now();
    if (now-lastVideoCaptureAt < profile.intervalMs) return;
    lastVideoCaptureAt = now;

    canvas.width = profile.width;
    canvas.height = profile.height;

    // Center-crop to match object-fit: cover at the tile's 4:3 aspect ratio,
    // so the captured frame looks identical to the live preview.
    const vw = video.videoWidth || profile.width;
    const vh = video.videoHeight || profile.height;
    const targetAspect = profile.width / profile.height;
    const sourceAspect = vw / vh;
    let sx = 0, sy = 0, sw = vw, sh = vh;
    if (sourceAspect > targetAspect) {
      sw = Math.round(vh * targetAspect);
      sx = Math.round((vw - sw) / 2);
    } else if (sourceAspect < targetAspect) {
      sh = Math.round(vw / targetAspect);
      sy = Math.round((vh - sh) / 2);
    }

    // Mirror horizontally to match the CSS scaleX(-1) on the local video preview,
    // so the replay looks the same as the live call.
    ctx.save();
    ctx.translate(profile.width, 0);
    ctx.scale(-1, 1);
    ctx.drawImage(video, sx, sy, sw, sh, 0, 0, profile.width, profile.height);
    ctx.restore();

    videoEncodeInFlight = true;
    canvas.toBlob((blob) => {
      videoEncodeInFlight = false;
      if (!blob || !ws || ws.readyState !== WebSocket.OPEN) return;
      // Backpressure: skip frame if WS send buffer is growing.
      if (ws.bufferedAmount > 100000) return;
      blob.arrayBuffer().then((buf) => {
        const payload = new Uint8Array(buf);
        const msg = new Uint8Array(1 + payload.length);
        msg[0] = MSG_VIDEO;
        msg.set(payload, 1);
        ws.send(msg);
      });
    }, imageFormat, profile.quality);
  }, 1000 / 30);
}

function getVideoCaptureProfile() {
  const participants = remoteVideos.size;
  if (participants >= 6) {
    return { width: 400, height: 300, quality: 0.42, intervalMs: 125 };
  }
  if (participants >= 3) {
    return { width: 480, height: 360, quality: 0.5, intervalMs: 100 };
  }
  return { width: 640, height: 480, quality: 0.65, intervalMs: 66 };
}

async function startAudioCapture() {
  if (!localStream) return;

  audioContext = new AudioContext({ sampleRate: 48000 });
  await audioContext.audioWorklet.addModule('/audio-worklet.js');

  const source = audioContext.createMediaStreamSource(localStream);
  audioWorklet = new AudioWorkletNode(audioContext, 'pcm-capture');

  audioWorklet.port.onmessage = (event) => {
    // Speaking indicator for local tile.
    const rms = event.data.rms || 0;
    const localTile = document.getElementById('local-tile');
    if (localTile) {
      localTile.classList.toggle('speaking', rms > 0.02);
    }

    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    if (!micEnabled) return;
    if (ws.bufferedAmount > 200000) return;
    const pcmData = new Uint8Array(event.data.pcm);
    const msg = new Uint8Array(1 + pcmData.length);
    msg[0] = MSG_AUDIO;
    msg.set(pcmData, 1);
    ws.send(msg);
  };

  source.connect(audioWorklet);
  // Don't connect worklet to destination (we don't want to hear ourselves).
}

function renderRemoteVideo(userId, imageData, replay) {
  const grid = replay
    ? document.getElementById('replay-grid')
    : document.getElementById('video-grid');
  ensureParticipantTile(userId, grid);

  const remote = remoteVideos.get(userId);
  if (!remote) return;

  queueImageFrame(imageData, remote);
}

function queueImageFrame(imageData, remote) {
  remote.pendingFrame = imageData;
  if (remote.renderScheduled || remote.decoding) return;
  remote.renderScheduled = true;
  requestAnimationFrame(() => drawPendingImageFrame(remote));
}

async function drawPendingImageFrame(remote) {
  remote.renderScheduled = false;
  if (remote.decoding || !remote.pendingFrame) return;

  const frame = remote.pendingFrame;
  remote.pendingFrame = null;
  remote.decoding = true;

  try {
    await renderImageFrame(frame, remote);
  } finally {
    remote.decoding = false;
  }

  if (remote.pendingFrame && !remote.renderScheduled) {
    remote.renderScheduled = true;
    requestAnimationFrame(() => drawPendingImageFrame(remote));
  }
}

async function renderImageFrame(imageData, remote) {
  if (typeof createImageBitmap === 'function') {
    try {
      const bitmap = await createImageBitmap(new Blob([imageData]));
      remote.ctx.drawImage(bitmap, 0, 0, remote.canvas.width, remote.canvas.height);
      if (typeof bitmap.close === 'function') bitmap.close();
      return;
    } catch (_) {
    }
  }

  await renderImageFrameFallback(imageData, remote);
}

function renderImageFrameFallback(imageData, remote) {
  return new Promise((resolve) => {
    const blob = new Blob([imageData]);
    const url = URL.createObjectURL(blob);
    const img = new Image();
    img.onload = () => {
      remote.ctx.drawImage(img, 0, 0, remote.canvas.width, remote.canvas.height);
      URL.revokeObjectURL(url);
      resolve();
    };
    img.onerror = () => {
      URL.revokeObjectURL(url);
      resolve();
    };
    img.src = url;
  });
}

// Shared AudioContext for all remote participants (browsers limit concurrent contexts).
let sharedPlaybackCtx = null;

function getPlaybackContext() {
  if (!sharedPlaybackCtx || sharedPlaybackCtx.state === 'closed') {
    sharedPlaybackCtx = new AudioContext({ sampleRate: 16000 });
  }
  return sharedPlaybackCtx;
}

function playRemoteAudio(userId, pcmData) {
  const ctx = getPlaybackContext();

  if (!remoteAudio.has(userId)) {
    remoteAudio.set(userId, { nextTime: 0 });
  }

  const remote = remoteAudio.get(userId);

  // Convert PCM16LE to Float32.
  const samples = pcmData.length / 2;
  const buffer = ctx.createBuffer(1, samples, 16000);
  const channelData = buffer.getChannelData(0);
  const view = new DataView(pcmData.buffer, pcmData.byteOffset, pcmData.byteLength);

  for (let i = 0; i < samples; i++) {
    const s = view.getInt16(i * 2, true);
    channelData[i] = s / 32768;
  }

  // Detect speaking from audio amplitude.
  let sumSq = 0;
  for (let i = 0; i < channelData.length; i++) {
    sumSq += channelData[i] * channelData[i];
  }
  const rms = Math.sqrt(sumSq / channelData.length);
  const tile = document.getElementById(`tile-${userId}`);
  if (tile) tile.classList.toggle('speaking', rms > 0.02);

  const source = ctx.createBufferSource();
  source.buffer = buffer;
  source.connect(ctx.destination);

  const now = ctx.currentTime;
  const startTime = Math.max(now, remote.nextTime);
  source.start(startTime);
  remote.nextTime = startTime + buffer.duration;
}

async function toggleScreenShare() {
  if (screenStream) {
    stopScreenShare();
    return;
  }

  try {
    screenStream = await navigator.mediaDevices.getDisplayMedia({
      video: { width: 1280, height: 720, frameRate: 10 },
    });
  } catch (e) {
    return; // User cancelled
  }

  document.getElementById('toggle-screen').classList.add('off');

  // Stop if user clicks browser's "Stop sharing" button.
  screenStream.getVideoTracks()[0].addEventListener('ended', stopScreenShare);

  // Show local screen preview.
  const grid = document.getElementById('video-grid');
  const tile = document.createElement('div');
  tile.className = 'video-tile screen-tile';
  tile.id = 'screen-tile-local';
  const previewVideo = document.createElement('video');
  previewVideo.srcObject = screenStream;
  previewVideo.muted = true;
  previewVideo.autoplay = true;
  previewVideo.playsInline = true;
  tile.appendChild(previewVideo);
  const label = document.createElement('div');
  label.className = 'video-label';
  label.textContent = 'You (screen)';
  tile.appendChild(label);
  grid.insertBefore(tile, grid.firstChild);

  const screenVideo = document.createElement('video');
  screenVideo.srcObject = screenStream;
  screenVideo.muted = true;
  screenVideo.play();

  const screenCanvas = document.createElement('canvas');
  screenCanvas.width = 1280;
  screenCanvas.height = 720;
  const ctx = screenCanvas.getContext('2d');

  screenCaptureInterval = setInterval(() => {
    if (!ws || ws.readyState !== WebSocket.OPEN || !screenStream) return;
    if (screenVideo.readyState < screenVideo.HAVE_CURRENT_DATA) return;
    if (screenEncodeInFlight) return;
    if (ws.bufferedAmount > 100000) return;

    ctx.drawImage(screenVideo, 0, 0, 1280, 720);
    screenEncodeInFlight = true;
    screenCanvas.toBlob((blob) => {
      screenEncodeInFlight = false;
      if (!blob || !ws || ws.readyState !== WebSocket.OPEN) return;
      blob.arrayBuffer().then((buf) => {
        const payload = new Uint8Array(buf);
        const msg = new Uint8Array(1 + payload.length);
        msg[0] = MSG_SCREEN;
        msg.set(payload, 1);
        ws.send(msg);
      });
    }, imageFormat, 0.7);
  }, 1000 / 10); // 10 fps for screen
}

function stopScreenShare() {
  if (screenCaptureInterval) {
    clearInterval(screenCaptureInterval);
    screenCaptureInterval = null;
  }
  screenEncodeInFlight = false;
  if (screenStream) {
    screenStream.getTracks().forEach(t => t.stop());
    screenStream = null;
  }
  document.getElementById('toggle-screen').classList.remove('off');

  const tile = document.getElementById('screen-tile-local');
  if (tile) tile.remove();

  // Notify others that screen sharing stopped.
  if (ws && ws.readyState === WebSocket.OPEN && localUserID) {
    const ctrl = JSON.stringify({ type: 'screen_stop', user_id: localUserID });
    const bytes = new TextEncoder().encode(ctrl);
    const msg = new Uint8Array(1 + bytes.length);
    msg[0] = MSG_CONTROL;
    msg.set(bytes, 1);
    ws.send(msg);
  }
}

const remoteScreens = new Map(); // userID -> { canvas, ctx }

function renderScreenShare(userId, jpegData, replay) {
  const grid = replay
    ? document.getElementById('replay-grid')
    : document.getElementById('video-grid');

  const tileId = `screen-${userId}`;
  if (!remoteScreens.has(userId)) {
    const tile = document.createElement('div');
    tile.className = 'video-tile screen-tile';
    tile.id = tileId;

    const canvas = document.createElement('canvas');
    canvas.width = 1280;
    canvas.height = 720;
    tile.appendChild(canvas);

    const label = document.createElement('div');
    label.className = 'video-label';
    label.textContent = userId + ' (screen)';
    tile.appendChild(label);

    // Insert screen tile at the beginning of the grid.
    grid.insertBefore(tile, grid.firstChild);
    remoteScreens.set(userId, {
      canvas,
      ctx: canvas.getContext('2d'),
      pendingFrame: null,
      renderScheduled: false,
      decoding: false,
    });
  }

  const remote = remoteScreens.get(userId);
  queueImageFrame(jpegData, remote);
}

function sendChat() {
  const input = document.getElementById('chat-input');
  const text = input.value.trim();
  if (!text || !ws || ws.readyState !== WebSocket.OPEN) return;

  input.value = '';

  const bytes = new TextEncoder().encode(text);
  const msg = new Uint8Array(1 + bytes.length);
  msg[0] = MSG_CHAT;
  msg.set(bytes, 1);
  ws.send(msg);
}

function handleChatMessage(msg) {
  const container = document.getElementById('chat-messages');
  if (!container) return;

  const bubble = document.createElement('div');
  const isMe = msg.user_id === localUserID;
  bubble.className = 'chat-bubble' + (isMe ? ' chat-mine' : '');

  const name = document.createElement('span');
  name.className = 'chat-name';
  name.textContent = isMe ? 'You' : msg.user_id;

  const text = document.createElement('span');
  text.className = 'chat-text';
  text.textContent = msg.text;

  bubble.appendChild(name);
  bubble.appendChild(text);
  container.appendChild(bubble);
  container.scrollTop = container.scrollHeight;

  // Show unread dot if chat is collapsed.
  const panel = document.getElementById('chat-panel');
  if (panel && panel.classList.contains('collapsed') && !isMe) {
    document.getElementById('toggle-chat').classList.add('has-unread');
  }
}

function bindReplayControls() {
  if (replayControlsBound) return;
  replayControlsBound = true;

  document.getElementById('replay-play-btn').addEventListener('click', toggleReplayPlayback);

  document.querySelectorAll('#speed-controls .speed-btn[data-speed]').forEach(btn => {
    btn.addEventListener('click', () => {
      const newSpeed = parseFloat(btn.dataset.speed);
      if (newSpeed === replaySpeed) return;
      replaySpeed = newSpeed;
      document.querySelectorAll('#speed-controls .speed-btn[data-speed]').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      seekReplay(replayCurrentTs || (replayTimeline ? replayTimeline.start : 0));
    });
  });

  document.getElementById('timeline-track').addEventListener('click', (e) => {
    if (!replayTimeline || !replayTimeline.end) return;
    const rect = e.currentTarget.getBoundingClientRect();
    const pct = (e.clientX - rect.left) / rect.width;
    const ts = replayTimeline.start + pct * (replayTimeline.end - replayTimeline.start);
    seekReplay(Math.floor(ts));
  });

  document.getElementById('go-live-btn').addEventListener('click', () => {
    goLive();
  });
}

async function initReplay(room, user) {
  try {
    const resp = await fetch(`/api/rooms/${encodeURIComponent(room)}/timeline`);
    replayTimeline = await resp.json();
  } catch (e) {
    replayTimeline = { start: 0, end: 0, participants: [], events: [] };
  }

  bindReplayControls();
  replayPaused = false;
  replayLoading = true;
  updateReplayPlayButton();
  setReplayStatus('Loading replay…');
  resetReplayChatState();
  clearReplayHandIndicators();
  resetReplayAudioState();
  document.querySelectorAll('#speed-controls .speed-btn[data-speed]').forEach(b => {
    b.classList.toggle('active', parseFloat(b.dataset.speed) === replaySpeed);
  });

  if (replayTimeline.end > replayTimeline.start) {
    document.getElementById('replay-duration').textContent =
      formatDuration(replayTimeline.end - replayTimeline.start);
  } else {
    document.getElementById('replay-duration').textContent = '00:00';
  }

  const focusContainer = document.getElementById('focus-controls');
  focusContainer.innerHTML = '';
  if (replayTimeline.participants && replayTimeline.participants.length > 1) {
    const allBtn = document.createElement('button');
    allBtn.className = 'focus-btn active';
    allBtn.textContent = 'All';
    allBtn.addEventListener('click', () => setReplayFocus(''));
    focusContainer.appendChild(allBtn);

    for (const pid of replayTimeline.participants) {
      const btn = document.createElement('button');
      btn.className = 'focus-btn';
      btn.textContent = pid;
      btn.dataset.focus = pid;
      btn.addEventListener('click', () => setReplayFocus(pid));
      focusContainer.appendChild(btn);
    }
  }

  replayCurrentTs = replayTimeline.start || 0;
  updateReplayProgress(replayCurrentTs);
  startReplayTimer();
  connectWebSocket(room, user, true, replayCurrentTs, replaySpeed);
}

function setReplayFocus(pid) {
  replayFocus = pid;
  document.querySelectorAll('.focus-btn').forEach(b => b.classList.remove('active'));
  if (pid === '') {
    const allBtn = document.querySelector('.focus-btn');
    if (allBtn) allBtn.classList.add('active');
  } else {
    const btn = document.querySelector(`.focus-btn[data-focus="${pid}"]`);
    if (btn) btn.classList.add('active');
  }
  seekReplay(replayCurrentTs || (replayTimeline ? replayTimeline.start : 0));
}

function seekReplay(timestamp) {
  replayCurrentTs = timestamp;
  replayPaused = false;
  replayLoading = true;
  updateReplayPlayButton();
  setReplayStatus('Seeking…');
  closeCurrentSocket();
  resetReplayChatState();
  clearReplayHandIndicators();
  resetReplayAudioState();
  connectWebSocket(replayRoom, replayUser, true, timestamp, replaySpeed);
  startReplayTimer();
}

function toggleReplayPlayback() {
  if (!replayTimeline) return;

  if (replayPaused) {
    replayPaused = false;
    replayLoading = true;
    updateReplayPlayButton();
    setReplayStatus('Loading replay…');
    connectWebSocket(replayRoom, replayUser, true, replayCurrentTs || replayTimeline.start || 0, replaySpeed);
    startReplayTimer();
    return;
  }

  replayPaused = true;
  replayLoading = false;
  updateReplayPlayButton();
  setReplayStatus('Paused');
  if (replayTimerInterval) clearInterval(replayTimerInterval);
  replayTimerInterval = null;
  closeCurrentSocket();
}

function startReplayTimer() {
  if (replayTimerInterval) clearInterval(replayTimerInterval);
  if (replayPaused) return;
  const startedAt = Date.now();
  const startTs = replayCurrentTs;

  replayTimerInterval = setInterval(() => {
    if (!replayTimeline) return;
    const elapsed = (Date.now() - startedAt) * replaySpeed;
    replayCurrentTs = startTs + elapsed;
    updateReplayProgress(replayCurrentTs);
  }, 200);
}

function updateReplayProgress(ts) {
  if (!replayTimeline || !replayTimeline.end || replayTimeline.end <= replayTimeline.start) return;
  const duration = replayTimeline.end - replayTimeline.start;
  const elapsed = Math.max(0, Math.min(ts - replayTimeline.start, duration));
  const pct = Math.max(0, Math.min(100, (elapsed / duration) * 100));

  document.getElementById('timeline-progress').style.width = pct + '%';
  document.getElementById('timeline-thumb').style.left = pct + '%';
  document.getElementById('replay-time').textContent = formatDuration(Math.max(0, elapsed));
}

function formatDuration(ms) {
  const totalSec = Math.floor(ms / 1000);
  const min = Math.floor(totalSec / 60);
  const sec = totalSec % 60;
  return `${min.toString().padStart(2, '0')}:${sec.toString().padStart(2, '0')}`;
}

async function goLive() {
  replayPaused = false;
  replayLoading = false;
  if (replayTimerInterval) clearInterval(replayTimerInterval);
  replayTimerInterval = null;
  closeCurrentSocket();
  remoteVideos.clear();
  remoteAudio.clear();
  remoteScreens.clear();

  document.getElementById('replay-page').classList.add('hidden');
  document.getElementById('room-page').classList.remove('hidden');
  document.getElementById('room-title').textContent = replayRoom;

  try {
    localStream = await navigator.mediaDevices.getUserMedia({
      video: { width: 640, height: 480, frameRate: 30 },
      audio: true,
    });
  } catch (e) {
    localStream = null;
  }

  const localVideo = document.getElementById('local-video');
  if (localStream) {
    localVideo.srcObject = localStream;
  } else {
    cameraEnabled = false;
    micEnabled = false;
    document.getElementById('local-tile').classList.add('camera-off');
    document.getElementById('toggle-camera').classList.add('off');
    document.getElementById('toggle-mic').classList.add('off');
    document.getElementById('room-status').textContent = 'No camera/mic — joining as viewer';
  }

  localUserID = replayUser;
  connectWebSocket(replayRoom, replayUser, false, 0);
}

function exportRecording() {
  if (!replayRoom) return;
  const url = `/api/rooms/${encodeURIComponent(replayRoom)}/export`;
  const btn = document.getElementById('export-btn');
  btn.textContent = 'Exporting...';
  btn.disabled = true;

  const a = document.createElement('a');
  a.href = url;
  a.download = `${replayRoom}.mp4`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);

  setTimeout(() => {
    btn.innerHTML = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg> Export MP4`;
    btn.disabled = false;
  }, 3000);
}

function toggleMetrics() {
  const panel = document.getElementById('metrics-panel');
  if (panel) panel.classList.toggle('metrics-hidden');
}

function updateMetricsOverlay(stats) {
  const panel = document.getElementById('metrics-panel');
  if (!panel) return;

  const totalRecords = stats.video_records + stats.audio_records + stats.screen_records;
  const totalBytes = stats.video_bytes + stats.audio_bytes + stats.screen_bytes;

  document.getElementById('stat-video-records').textContent = stats.video_records.toLocaleString();
  document.getElementById('stat-audio-records').textContent = stats.audio_records.toLocaleString();
  document.getElementById('stat-total-records').textContent = totalRecords.toLocaleString();
  document.getElementById('stat-total-bytes').textContent = formatBytesCompact(totalBytes);
}

function formatBytesCompact(b) {
  if (b < 1024) return b + ' B';
  if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
  return (b / (1024 * 1024)).toFixed(1) + ' MB';
}

function toggleChat() {
  const panel = document.getElementById('chat-panel');
  panel.classList.toggle('collapsed');
  document.getElementById('toggle-chat').classList.remove('has-unread');
  if (!panel.classList.contains('collapsed')) {
    document.getElementById('chat-input').focus();
  }
}
