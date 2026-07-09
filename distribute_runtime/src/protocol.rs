//! Binary wire protocol for master <-> worker data channels.
//!
//! Every data-channel message is one frame:
//!
//! ```text
//! [magic u8][version u8][ftype u8][reserved u8]
//! [id_len u16 LE][transfer id bytes (utf8)]
//! [frame_seq u32 LE][total_frames u32 LE]
//! [payload_len u32 LE][payload bytes]
//! ```
//!
//! Frames of one transfer reassemble into a payload. Task payloads carry a
//! JSON header plus length-prefixed sections (wasm, js glue, input). Result
//! payloads carry a JSON header plus the raw result body.

use serde::{Deserialize, Serialize};

pub const FRAME_MAGIC: u8 = 0xA5;
pub const PROTO_VERSION: u8 = 1;
pub const FRAME_TASK: u8 = 1;
pub const FRAME_RESULT: u8 = 2;
/// Keep frames comfortably under common SCTP message-size limits.
pub const MAX_FRAME_PAYLOAD: usize = 60_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskHeader {
    pub job_id: String,
    pub task_id: String,
    pub chunk_index: u32,
    pub map_function: String,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultHeader {
    pub task_id: String,
    pub chunk_index: u32,
    pub worker_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub meta: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub ftype: u8,
    pub transfer_id: String,
    pub frame_seq: u32,
    pub total_frames: u32,
    pub payload: Vec<u8>,
}

pub fn encode_frame(f: &Frame) -> Vec<u8> {
    let id = f.transfer_id.as_bytes();
    let mut out = Vec::with_capacity(4 + 2 + id.len() + 12 + f.payload.len());
    out.push(FRAME_MAGIC);
    out.push(PROTO_VERSION);
    out.push(f.ftype);
    out.push(0);
    out.extend_from_slice(&(id.len() as u16).to_le_bytes());
    out.extend_from_slice(id);
    out.extend_from_slice(&f.frame_seq.to_le_bytes());
    out.extend_from_slice(&f.total_frames.to_le_bytes());
    out.extend_from_slice(&(f.payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&f.payload);
    out
}

pub fn decode_frame(buf: &[u8]) -> Result<Frame, String> {
    if buf.len() < 4 {
        return Err("frame too short".into());
    }
    if buf[0] != FRAME_MAGIC {
        return Err(format!("bad magic byte 0x{:02x}", buf[0]));
    }
    if buf[1] != PROTO_VERSION {
        return Err(format!("unsupported protocol version {}", buf[1]));
    }
    let ftype = buf[2];
    let mut pos = 4usize;
    let id_len = read_u16(buf, &mut pos)? as usize;
    if pos + id_len > buf.len() {
        return Err("truncated transfer id".into());
    }
    let transfer_id = String::from_utf8(buf[pos..pos + id_len].to_vec())
        .map_err(|_| "transfer id is not utf8".to_string())?;
    pos += id_len;
    let frame_seq = read_u32(buf, &mut pos)?;
    let total_frames = read_u32(buf, &mut pos)?;
    let payload_len = read_u32(buf, &mut pos)? as usize;
    if pos + payload_len > buf.len() {
        return Err("truncated payload".into());
    }
    let payload = buf[pos..pos + payload_len].to_vec();
    Ok(Frame {
        ftype,
        transfer_id,
        frame_seq,
        total_frames,
        payload,
    })
}

/// Split a payload into encoded frames ready to send.
pub fn split_into_frames(ftype: u8, transfer_id: &str, payload: &[u8]) -> Vec<Vec<u8>> {
    let total = payload.len().div_ceil(MAX_FRAME_PAYLOAD).max(1) as u32;
    let mut frames = Vec::with_capacity(total as usize);
    for seq in 0..total {
        let start = seq as usize * MAX_FRAME_PAYLOAD;
        let end = (start + MAX_FRAME_PAYLOAD).min(payload.len());
        frames.push(encode_frame(&Frame {
            ftype,
            transfer_id: transfer_id.to_string(),
            frame_seq: seq,
            total_frames: total,
            payload: payload[start..end].to_vec(),
        }));
    }
    frames
}

/// Reassembles frames into complete payloads. One instance per data channel.
#[derive(Default)]
pub struct Reassembler {
    partial: std::collections::HashMap<String, Partial>,
}

struct Partial {
    ftype: u8,
    parts: Vec<Option<Vec<u8>>>,
    received: u32,
}

impl Reassembler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one frame. Returns the full payload when a transfer completes.
    pub fn accept(&mut self, frame: Frame) -> Option<(u8, String, Vec<u8>)> {
        if frame.total_frames == 0 || frame.frame_seq >= frame.total_frames {
            return None;
        }
        let entry = self
            .partial
            .entry(frame.transfer_id.clone())
            .or_insert_with(|| Partial {
                ftype: frame.ftype,
                parts: vec![None; frame.total_frames as usize],
                received: 0,
            });
        if entry.parts.len() != frame.total_frames as usize {
            // Inconsistent framing; drop the transfer.
            self.partial.remove(&frame.transfer_id);
            return None;
        }
        let slot = &mut entry.parts[frame.frame_seq as usize];
        if slot.is_none() {
            *slot = Some(frame.payload);
            entry.received += 1;
        }
        if entry.received == entry.parts.len() as u32 {
            let done = self.partial.remove(&frame.transfer_id).unwrap();
            let mut payload = Vec::new();
            for part in done.parts {
                payload.extend_from_slice(&part.unwrap());
            }
            return Some((done.ftype, frame.transfer_id, payload));
        }
        None
    }
}

/// Task payload: `[u32 header_len][header json][u32 wasm_len][wasm][u32 glue_len][glue][u32 input_len][input]`
pub fn encode_task_payload(
    header: &TaskHeader,
    wasm: &[u8],
    glue: &[u8],
    input: &[u8],
) -> Vec<u8> {
    let header_json = serde_json::to_vec(header).expect("task header serializes");
    let mut out =
        Vec::with_capacity(16 + header_json.len() + wasm.len() + glue.len() + input.len());
    out.extend_from_slice(&(header_json.len() as u32).to_le_bytes());
    out.extend_from_slice(&header_json);
    out.extend_from_slice(&(wasm.len() as u32).to_le_bytes());
    out.extend_from_slice(wasm);
    out.extend_from_slice(&(glue.len() as u32).to_le_bytes());
    out.extend_from_slice(glue);
    out.extend_from_slice(&(input.len() as u32).to_le_bytes());
    out.extend_from_slice(input);
    out
}

pub struct TaskPayload {
    pub header: TaskHeader,
    pub wasm: Vec<u8>,
    pub glue: Vec<u8>,
    pub input: Vec<u8>,
}

pub fn decode_task_payload(buf: &[u8]) -> Result<TaskPayload, String> {
    let mut pos = 0usize;
    let header_bytes = read_section(buf, &mut pos)?;
    let header: TaskHeader =
        serde_json::from_slice(&header_bytes).map_err(|e| format!("bad task header: {e}"))?;
    let wasm = read_section(buf, &mut pos)?;
    let glue = read_section(buf, &mut pos)?;
    let input = read_section(buf, &mut pos)?;
    Ok(TaskPayload {
        header,
        wasm,
        glue,
        input,
    })
}

/// Result payload: `[u32 header_len][header json][body bytes]`
pub fn encode_result_payload(header: &ResultHeader, body: &[u8]) -> Vec<u8> {
    let header_json = serde_json::to_vec(header).expect("result header serializes");
    let mut out = Vec::with_capacity(4 + header_json.len() + body.len());
    out.extend_from_slice(&(header_json.len() as u32).to_le_bytes());
    out.extend_from_slice(&header_json);
    out.extend_from_slice(body);
    out
}

pub fn decode_result_payload(buf: &[u8]) -> Result<(ResultHeader, Vec<u8>), String> {
    let mut pos = 0usize;
    let header_bytes = read_section(buf, &mut pos)?;
    let header: ResultHeader =
        serde_json::from_slice(&header_bytes).map_err(|e| format!("bad result header: {e}"))?;
    Ok((header, buf[pos..].to_vec()))
}

fn read_u16(buf: &[u8], pos: &mut usize) -> Result<u16, String> {
    if *pos + 2 > buf.len() {
        return Err("truncated u16".into());
    }
    let v = u16::from_le_bytes([buf[*pos], buf[*pos + 1]]);
    *pos += 2;
    Ok(v)
}

fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32, String> {
    if *pos + 4 > buf.len() {
        return Err("truncated u32".into());
    }
    let v = u32::from_le_bytes([buf[*pos], buf[*pos + 1], buf[*pos + 2], buf[*pos + 3]]);
    *pos += 4;
    Ok(v)
}

fn read_section(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>, String> {
    let len = read_u32(buf, pos)? as usize;
    if *pos + len > buf.len() {
        return Err("truncated section".into());
    }
    let out = buf[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let f = Frame {
            ftype: FRAME_TASK,
            transfer_id: "task_1".into(),
            frame_seq: 2,
            total_frames: 5,
            payload: vec![1, 2, 3, 4],
        };
        let enc = encode_frame(&f);
        let dec = decode_frame(&enc).unwrap();
        assert_eq!(dec.ftype, FRAME_TASK);
        assert_eq!(dec.transfer_id, "task_1");
        assert_eq!(dec.frame_seq, 2);
        assert_eq!(dec.total_frames, 5);
        assert_eq!(dec.payload, vec![1, 2, 3, 4]);
    }

    #[test]
    fn rejects_bad_magic() {
        let f = Frame {
            ftype: FRAME_RESULT,
            transfer_id: "x".into(),
            frame_seq: 0,
            total_frames: 1,
            payload: vec![],
        };
        let mut enc = encode_frame(&f);
        enc[0] = 0x00;
        assert!(decode_frame(&enc).is_err());
    }

    #[test]
    fn split_and_reassemble_out_of_order() {
        let payload: Vec<u8> = (0..(MAX_FRAME_PAYLOAD * 2 + 100))
            .map(|i| (i % 251) as u8)
            .collect();
        let frames = split_into_frames(FRAME_RESULT, "t42", &payload);
        assert_eq!(frames.len(), 3);

        let mut re = Reassembler::new();
        // Deliver out of order: 2, 0, 1
        assert!(re.accept(decode_frame(&frames[2]).unwrap()).is_none());
        assert!(re.accept(decode_frame(&frames[0]).unwrap()).is_none());
        let (ftype, id, out) = re.accept(decode_frame(&frames[1]).unwrap()).unwrap();
        assert_eq!(ftype, FRAME_RESULT);
        assert_eq!(id, "t42");
        assert_eq!(out, payload);
    }

    #[test]
    fn duplicate_frames_ignored() {
        let payload = vec![7u8; MAX_FRAME_PAYLOAD + 1];
        let frames = split_into_frames(FRAME_TASK, "dup", &payload);
        assert_eq!(frames.len(), 2);
        let mut re = Reassembler::new();
        assert!(re.accept(decode_frame(&frames[0]).unwrap()).is_none());
        assert!(re.accept(decode_frame(&frames[0]).unwrap()).is_none());
        let done = re.accept(decode_frame(&frames[1]).unwrap());
        assert!(done.is_some());
        assert_eq!(done.unwrap().2, payload);
    }

    #[test]
    fn empty_payload_is_one_frame() {
        let frames = split_into_frames(FRAME_TASK, "empty", &[]);
        assert_eq!(frames.len(), 1);
        let mut re = Reassembler::new();
        let (_, _, out) = re.accept(decode_frame(&frames[0]).unwrap()).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn task_payload_roundtrip() {
        let header = TaskHeader {
            job_id: "job1".into(),
            task_id: "t1".into(),
            chunk_index: 3,
            map_function: "grayscale_frame_rgba".into(),
            meta: serde_json::json!({"width": 2, "height": 2}),
        };
        let enc = encode_task_payload(&header, &[9, 9], &[], &[1, 2, 3]);
        let dec = decode_task_payload(&enc).unwrap();
        assert_eq!(dec.header.task_id, "t1");
        assert_eq!(dec.header.chunk_index, 3);
        assert_eq!(dec.wasm, vec![9, 9]);
        assert!(dec.glue.is_empty());
        assert_eq!(dec.input, vec![1, 2, 3]);
    }

    #[test]
    fn result_payload_roundtrip() {
        let header = ResultHeader {
            task_id: "t9".into(),
            chunk_index: 0,
            worker_id: "w1".into(),
            error: Some("boom".into()),
            meta: serde_json::Value::Null,
        };
        let enc = encode_result_payload(&header, b"body");
        let (h, body) = decode_result_payload(&enc).unwrap();
        assert_eq!(h.error.as_deref(), Some("boom"));
        assert_eq!(body, b"body");
    }
}
