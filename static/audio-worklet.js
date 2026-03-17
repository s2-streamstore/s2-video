// AudioWorklet processor for capturing PCM audio.
// This file is loaded as a module by AudioWorkletNode.

class PCMCaptureProcessor extends AudioWorkletProcessor {
  constructor() {
    super();
    this._buffer = [];
    this._bufferSize = 0;
    // Collect ~50ms of audio at a time (at native sample rate).
    this._targetSamples = Math.floor(sampleRate * 0.05);
  }

  process(inputs) {
    const input = inputs[0];
    if (!input || !input[0]) return true;

    // Take first channel (mono).
    const channelData = input[0];
    this._buffer.push(new Float32Array(channelData));
    this._bufferSize += channelData.length;

    if (this._bufferSize >= this._targetSamples) {
      // Concatenate buffer.
      const fullBuffer = new Float32Array(this._bufferSize);
      let offset = 0;
      for (const chunk of this._buffer) {
        fullBuffer.set(chunk, offset);
        offset += chunk.length;
      }
      this._buffer = [];
      this._bufferSize = 0;

      // Resample to 16kHz.
      const resampled = resample(fullBuffer, sampleRate, 16000);

      // Compute RMS amplitude for speaking detection.
      let sumSq = 0;
      for (let i = 0; i < resampled.length; i++) {
        sumSq += resampled[i] * resampled[i];
      }
      const rms = Math.sqrt(sumSq / resampled.length);

      // Convert to PCM16LE.
      const pcm = float32ToPCM16LE(resampled);
      this.port.postMessage({ pcm: pcm.buffer, rms }, [pcm.buffer]);
    }

    return true;
  }
}

function resample(input, fromRate, toRate) {
  if (fromRate === toRate) return input;
  const ratio = fromRate / toRate;
  const outputLength = Math.floor(input.length / ratio);
  const output = new Float32Array(outputLength);
  for (let i = 0; i < outputLength; i++) {
    const srcIdx = i * ratio;
    const lo = Math.floor(srcIdx);
    const hi = Math.min(lo + 1, input.length - 1);
    const frac = srcIdx - lo;
    output[i] = input[lo] * (1 - frac) + input[hi] * frac;
  }
  return output;
}

function float32ToPCM16LE(float32) {
  const buffer = new ArrayBuffer(float32.length * 2);
  const view = new DataView(buffer);
  for (let i = 0; i < float32.length; i++) {
    const s = Math.max(-1, Math.min(1, float32[i]));
    view.setInt16(i * 2, s < 0 ? s * 0x8000 : s * 0x7FFF, true);
  }
  return new Uint8Array(buffer);
}

registerProcessor('pcm-capture', PCMCaptureProcessor);
