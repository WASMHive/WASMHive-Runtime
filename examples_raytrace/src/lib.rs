//! Distributed path tracer: the WASM side.
//!
//! Workers receive one tile per task as a JSON `TileJob` (scene + tile rect +
//! image settings) and return raw RGBA bytes for that tile. Rendering is
//! deterministic: the per-pixel RNG is seeded from pixel coordinates, so a
//! retried or duplicated tile produces identical bytes.

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// Scene description (shared between master and workers via JSON)
// ---------------------------------------------------------------------------

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Material {
    Lambertian { albedo: [f32; 3] },
    Metal { albedo: [f32; 3], fuzz: f32 },
    Dielectric { ref_idx: f32 },
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Sphere {
    pub center: [f32; 3],
    pub radius: f32,
    pub material: Material,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Scene {
    pub spheres: Vec<Sphere>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TileJob {
    pub width: u32,
    pub height: u32,
    pub samples: u32,
    pub max_depth: u32,
    pub tile_x: u32,
    pub tile_y: u32,
    pub tile_w: u32,
    pub tile_h: u32,
    pub scene: Scene,
}

// ---------------------------------------------------------------------------
// Deterministic RNG (xorshift64*), no external crates
// ---------------------------------------------------------------------------

pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
    fn range(&mut self, lo: f32, hi: f32) -> f32 {
        lo + (hi - lo) * self.next_f32()
    }
}

// ---------------------------------------------------------------------------
// Small vector helpers
// ---------------------------------------------------------------------------

type V3 = [f32; 3];

fn add(a: V3, b: V3) -> V3 {
    [a[0] + b[0], a[1] + b[1], a[2] + b[2]]
}
fn sub(a: V3, b: V3) -> V3 {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}
fn scale(a: V3, s: f32) -> V3 {
    [a[0] * s, a[1] * s, a[2] * s]
}
fn mul(a: V3, b: V3) -> V3 {
    [a[0] * b[0], a[1] * b[1], a[2] * b[2]]
}
fn dot(a: V3, b: V3) -> f32 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
fn cross(a: V3, b: V3) -> V3 {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}
fn length(a: V3) -> f32 {
    dot(a, a).sqrt()
}
fn normalize(a: V3) -> V3 {
    scale(a, 1.0 / length(a))
}
fn near_zero(a: V3) -> bool {
    a[0].abs() < 1e-7 && a[1].abs() < 1e-7 && a[2].abs() < 1e-7
}
fn reflect(v: V3, n: V3) -> V3 {
    sub(v, scale(n, 2.0 * dot(v, n)))
}
fn refract(uv: V3, n: V3, etai_over_etat: f32) -> V3 {
    let cos_theta = dot(scale(uv, -1.0), n).min(1.0);
    let r_out_perp = scale(add(uv, scale(n, cos_theta)), etai_over_etat);
    let r_out_parallel = scale(n, -(1.0 - dot(r_out_perp, r_out_perp)).abs().sqrt());
    add(r_out_perp, r_out_parallel)
}
fn random_unit_vector(rng: &mut Rng) -> V3 {
    // Rejection sample the unit sphere, then normalize.
    loop {
        let p = [
            rng.range(-1.0, 1.0),
            rng.range(-1.0, 1.0),
            rng.range(-1.0, 1.0),
        ];
        let len2 = dot(p, p);
        if len2 > 1e-8 && len2 <= 1.0 {
            return scale(p, 1.0 / len2.sqrt());
        }
    }
}

// ---------------------------------------------------------------------------
// Camera (fixed classic view)
// ---------------------------------------------------------------------------

struct Camera {
    origin: V3,
    lower_left: V3,
    horizontal: V3,
    vertical: V3,
}

impl Camera {
    fn new(aspect: f32) -> Self {
        let lookfrom = [13.0, 2.0, 3.0];
        let lookat = [0.0, 0.0, 0.0];
        let vup = [0.0, 1.0, 0.0];
        let vfov: f32 = 20.0;

        let theta = vfov.to_radians();
        let h = (theta / 2.0).tan();
        let viewport_height = 2.0 * h;
        let viewport_width = aspect * viewport_height;

        let w = normalize(sub(lookfrom, lookat));
        let u = normalize(cross(vup, w));
        let v = cross(w, u);

        let origin = lookfrom;
        let horizontal = scale(u, viewport_width);
        let vertical = scale(v, viewport_height);
        let lower_left = sub(
            sub(sub(origin, scale(horizontal, 0.5)), scale(vertical, 0.5)),
            w,
        );
        Camera {
            origin,
            lower_left,
            horizontal,
            vertical,
        }
    }

    fn ray(&self, s: f32, t: f32) -> (V3, V3) {
        let dir = sub(
            add(
                add(self.lower_left, scale(self.horizontal, s)),
                scale(self.vertical, t),
            ),
            self.origin,
        );
        (self.origin, dir)
    }
}

// ---------------------------------------------------------------------------
// Intersection and shading
// ---------------------------------------------------------------------------

struct Hit {
    p: V3,
    normal: V3,
    front_face: bool,
    sphere_index: usize,
}

fn hit_scene(scene: &Scene, ro: V3, rd: V3, t_min: f32, t_max: f32) -> Option<Hit> {
    let mut closest = t_max;
    let mut best: Option<Hit> = None;
    for (i, s) in scene.spheres.iter().enumerate() {
        let oc = sub(ro, s.center);
        let a = dot(rd, rd);
        let half_b = dot(oc, rd);
        let c = dot(oc, oc) - s.radius * s.radius;
        let disc = half_b * half_b - a * c;
        if disc < 0.0 {
            continue;
        }
        let sqrtd = disc.sqrt();
        let mut root = (-half_b - sqrtd) / a;
        if root < t_min || root > closest {
            root = (-half_b + sqrtd) / a;
            if root < t_min || root > closest {
                continue;
            }
        }
        closest = root;
        let p = add(ro, scale(rd, root));
        let outward = scale(sub(p, s.center), 1.0 / s.radius);
        let front_face = dot(rd, outward) < 0.0;
        best = Some(Hit {
            p,
            normal: if front_face { outward } else { scale(outward, -1.0) },
            front_face,
            sphere_index: i,
        });
    }
    best
}

fn schlick(cosine: f32, ref_idx: f32) -> f32 {
    let r0 = ((1.0 - ref_idx) / (1.0 + ref_idx)).powi(2);
    r0 + (1.0 - r0) * (1.0 - cosine).powi(5)
}

fn ray_color(scene: &Scene, mut ro: V3, mut rd: V3, max_depth: u32, rng: &mut Rng) -> V3 {
    let mut attenuation: V3 = [1.0, 1.0, 1.0];
    for _ in 0..max_depth {
        let Some(hit) = hit_scene(scene, ro, rd, 0.001, f32::INFINITY) else {
            // Sky gradient
            let unit = normalize(rd);
            let t = 0.5 * (unit[1] + 1.0);
            let sky = add(scale([1.0, 1.0, 1.0], 1.0 - t), scale([0.5, 0.7, 1.0], t));
            return mul(attenuation, sky);
        };
        match &scene.spheres[hit.sphere_index].material {
            Material::Lambertian { albedo } => {
                let mut dir = add(hit.normal, random_unit_vector(rng));
                if near_zero(dir) {
                    dir = hit.normal;
                }
                attenuation = mul(attenuation, *albedo);
                ro = hit.p;
                rd = dir;
            }
            Material::Metal { albedo, fuzz } => {
                let reflected = reflect(normalize(rd), hit.normal);
                let dir = add(reflected, scale(random_unit_vector(rng), *fuzz));
                if dot(dir, hit.normal) <= 0.0 {
                    return [0.0, 0.0, 0.0]; // absorbed
                }
                attenuation = mul(attenuation, *albedo);
                ro = hit.p;
                rd = dir;
            }
            Material::Dielectric { ref_idx } => {
                let ratio = if hit.front_face { 1.0 / ref_idx } else { *ref_idx };
                let unit = normalize(rd);
                let cos_theta = dot(scale(unit, -1.0), hit.normal).min(1.0);
                let sin_theta = (1.0 - cos_theta * cos_theta).sqrt();
                let cannot_refract = ratio * sin_theta > 1.0;
                let dir = if cannot_refract || schlick(cos_theta, ratio) > rng.next_f32() {
                    reflect(unit, hit.normal)
                } else {
                    refract(unit, hit.normal, ratio)
                };
                ro = hit.p;
                rd = dir;
            }
        }
    }
    [0.0, 0.0, 0.0] // ran out of bounces
}

// ---------------------------------------------------------------------------
// Tile rendering
// ---------------------------------------------------------------------------

pub fn render_tile_impl(job: &TileJob) -> Vec<u8> {
    let camera = Camera::new(job.width as f32 / job.height as f32);
    let mut out = Vec::with_capacity((job.tile_w * job.tile_h * 4) as usize);
    for j in 0..job.tile_h {
        let py = job.tile_y + j;
        for i in 0..job.tile_w {
            let px = job.tile_x + i;
            let mut col: V3 = [0.0, 0.0, 0.0];
            for s in 0..job.samples {
                // Deterministic per pixel-sample: retries render identical bytes.
                let seed = ((px as u64) << 40) ^ ((py as u64) << 20) ^ (s as u64) ^ 0x9E3779B9;
                let mut rng = Rng::new(seed);
                let u = (px as f32 + rng.next_f32()) / job.width as f32;
                let v = 1.0 - (py as f32 + rng.next_f32()) / job.height as f32;
                let (ro, rd) = camera.ray(u, v);
                col = add(col, ray_color(&job.scene, ro, rd, job.max_depth, &mut rng));
            }
            let inv = 1.0 / job.samples as f32;
            for c in 0..3 {
                // Average + gamma 2
                let v = (col[c] * inv).max(0.0).sqrt().min(0.999);
                out.push((v * 256.0) as u8);
            }
            out.push(255);
        }
    }
    out
}

/// The map function shipped to workers: JSON TileJob in, raw RGBA out.
#[wasm_bindgen]
pub fn render_tile(input: Vec<u8>, _meta: JsValue) -> Vec<u8> {
    let job: TileJob = serde_json::from_slice(&input).expect("invalid TileJob payload");
    render_tile_impl(&job)
}

// ---------------------------------------------------------------------------
// Scene generation (used by the master; deterministic per seed)
// ---------------------------------------------------------------------------

pub fn sample_scene(seed: u64) -> Scene {
    let mut rng = Rng::new(seed);
    let mut spheres = vec![
        // Ground
        Sphere {
            center: [0.0, -1000.0, 0.0],
            radius: 1000.0,
            material: Material::Lambertian {
                albedo: [0.5, 0.5, 0.5],
            },
        },
        // The big three
        Sphere {
            center: [0.0, 1.0, 0.0],
            radius: 1.0,
            material: Material::Dielectric { ref_idx: 1.5 },
        },
        Sphere {
            center: [-4.0, 1.0, 0.0],
            radius: 1.0,
            material: Material::Lambertian {
                albedo: [0.4, 0.2, 0.1],
            },
        },
        Sphere {
            center: [4.0, 1.0, 0.0],
            radius: 1.0,
            material: Material::Metal {
                albedo: [0.7, 0.6, 0.5],
                fuzz: 0.0,
            },
        },
    ];

    for a in -4..5i32 {
        for b in -2..3i32 {
            let center = [
                a as f32 * 1.6 + 0.9 * rng.next_f32(),
                0.2,
                b as f32 * 1.6 + 0.9 * rng.next_f32(),
            ];
            // Keep clear of the big three
            if [[0.0f32, 1.0, 0.0], [-4.0, 1.0, 0.0], [4.0, 1.0, 0.0]]
                .iter()
                .any(|big| length(sub(center, *big)) < 1.3)
            {
                continue;
            }
            let pick = rng.next_f32();
            let material = if pick < 0.6 {
                Material::Lambertian {
                    albedo: [
                        rng.next_f32() * rng.next_f32(),
                        rng.next_f32() * rng.next_f32(),
                        rng.next_f32() * rng.next_f32(),
                    ],
                }
            } else if pick < 0.85 {
                Material::Metal {
                    albedo: [
                        0.5 + 0.5 * rng.next_f32(),
                        0.5 + 0.5 * rng.next_f32(),
                        0.5 + 0.5 * rng.next_f32(),
                    ],
                    fuzz: 0.4 * rng.next_f32(),
                }
            } else {
                Material::Dielectric { ref_idx: 1.5 }
            };
            spheres.push(Sphere {
                center,
                radius: 0.2,
                material,
            });
        }
    }
    Scene { spheres }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tile_renders_expected_size_and_is_deterministic() {
        let job = TileJob {
            width: 64,
            height: 36,
            samples: 4,
            max_depth: 4,
            tile_x: 16,
            tile_y: 8,
            tile_w: 8,
            tile_h: 8,
            scene: sample_scene(42),
        };
        let a = render_tile_impl(&job);
        let b = render_tile_impl(&job);
        assert_eq!(a.len(), 8 * 8 * 4);
        assert_eq!(a, b);
        // Alpha channel is opaque
        assert!(a.chunks(4).all(|px| px[3] == 255));
    }

    #[test]
    fn scene_is_deterministic_per_seed() {
        let a = sample_scene(7);
        let b = sample_scene(7);
        assert_eq!(a.spheres.len(), b.spheres.len());
        assert!(a.spheres.len() > 10);
    }
}
