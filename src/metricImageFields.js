/**
 * Heartbeat metrics: resolve canonical image ref + sha256 digest from Docker inspect data.
 * listContainers often returns repo:tag or a short id; image inspect has RepoDigests / Id.
 */

/**
 * Pure merge: listContainers Image string + container inspect Image ref + `docker.getImage().inspect()` body.
 *
 * @param {string} listImage - `c.Image` from docker.listContainers
 * @param {string} containerImageRef - `container.inspect().Image` (image id / digest ref)
 * @param {object} imageInspect - result of docker.getImage(ref).inspect()
 * @returns {{ image: string, image_digest: string|null }}
 */
export function resolveMetricImageFields(listImage, containerImageRef, imageInspect) {
  const image = String(listImage || "").trim();
  const out = { image, image_digest: null };
  const imgInsp = imageInspect && typeof imageInspect === "object" ? imageInspect : {};
  const repoDigests = Array.isArray(imgInsp.RepoDigests) ? imgInsp.RepoDigests : [];

  for (const rd of repoDigests) {
    const s = String(rd);
    const at = s.lastIndexOf("@sha256:");
    if (at >= 0) {
      const hex = s.slice(at + "@sha256:".length).toLowerCase();
      if (/^[a-f0-9]{64}$/.test(hex)) {
        out.image_digest = `sha256:${hex}`;
        if (!image.includes("@sha256:")) {
          const bare = image.split("@")[0];
          out.image = bare ? `${bare}@sha256:${hex}` : `sha256:${hex}`;
        }
        return out;
      }
    }
  }

  const id = String(imgInsp.Id || containerImageRef || "");
  const m = id.match(/sha256:([a-f0-9]{64})/i);
  if (m) {
    out.image_digest = `sha256:${m[1].toLowerCase()}`;
    if (!image || (!image.includes("sha256:") && !/^[a-f0-9]{64}$/i.test(image))) {
      out.image = out.image_digest;
    }
  }

  return out;
}

/**
 * Async: same as heartbeat path (container inspect + image inspect).
 *
 * @param {import('dockerode')} docker
 * @param {string} containerId
 * @param {string} listImage
 */
export async function getMetricImageFields(docker, containerId, listImage) {
  const image = String(listImage || "").trim();
  const fallback = { image, image_digest: null };
  try {
    const cont = docker.getContainer(containerId);
    const insp = await cont.inspect();
    const ref = insp.Image;
    const img = docker.getImage(ref);
    const imgInsp = await img.inspect();
    return resolveMetricImageFields(listImage, ref, imgInsp);
  } catch {
    return fallback;
  }
}
