import assert from "node:assert/strict";
import { test } from "node:test";
import { getMetricImageFields, resolveMetricImageFields } from "./metricImageFields.js";

const DIGEST = "8223df9556845708255fdcf6d284e8ed4c70f86839d7d0147b5864a111b87c36";
const REPO_DIGEST = `778954507986.dkr.ecr.ap-southeast-2.amazonaws.com/aionqa-cure@sha256:${DIGEST}`;

test("resolveMetricImageFields: ECR repo:tag + RepoDigests → image_digest and image@sha256", () => {
  const listImage = "778954507986.dkr.ecr.ap-southeast-2.amazonaws.com/aionqa-cure:latest";
  const ref = `sha256:${DIGEST}`;
  const imgInsp = {
    Id: ref,
    RepoDigests: [REPO_DIGEST],
  };
  const out = resolveMetricImageFields(listImage, ref, imgInsp);
  assert.equal(out.image_digest, `sha256:${DIGEST}`);
  assert.equal(out.image, `${listImage.split("@")[0]}@sha256:${DIGEST}`);
});

test("resolveMetricImageFields: no RepoDigests but Id is sha256 → digest from Id", () => {
  const ref = `sha256:${DIGEST}`;
  const out = resolveMetricImageFields("a8dd7bb0ff2c", ref, { Id: ref, RepoDigests: [] });
  assert.equal(out.image_digest, `sha256:${DIGEST}`);
  assert.equal(out.image, `sha256:${DIGEST}`);
});

test("resolveMetricImageFields: list image already has @sha256 → keep image string, still set digest", () => {
  const tagged = `myreg/img:tag@sha256:${DIGEST}`;
  const ref = `sha256:${DIGEST}`;
  const out = resolveMetricImageFields(tagged, ref, {
    Id: ref,
    RepoDigests: [REPO_DIGEST],
  });
  assert.equal(out.image_digest, `sha256:${DIGEST}`);
  assert.equal(out.image, tagged);
});

test("resolveMetricImageFields: empty inspect → pass-through list image", () => {
  const out = resolveMetricImageFields("busybox:latest", "sha256:abc", {});
  assert.equal(out.image, "busybox:latest");
  assert.equal(out.image_digest, null);
});

test("getMetricImageFields: uses dockerode inspect chain (mock)", async () => {
  const ref = `sha256:${DIGEST}`;
  const docker = {
    getContainer: () => ({
      inspect: async () => ({ Image: ref }),
    }),
    getImage: () => ({
      inspect: async () => ({
        Id: ref,
        RepoDigests: [REPO_DIGEST],
      }),
    }),
  };
  const listImage = "778954507986.dkr.ecr.ap-southeast-2.amazonaws.com/aionqa-cure:latest";
  const out = await getMetricImageFields(docker, "container-id-1", listImage);
  assert.equal(out.image_digest, `sha256:${DIGEST}`);
  assert.ok(out.image.includes("@sha256:"));
});

test("getMetricImageFields: docker throws → fallback list image only", async () => {
  const docker = {
    getContainer: () => ({
      inspect: async () => {
        throw new Error("no docker");
      },
    }),
  };
  const out = await getMetricImageFields(docker, "x", "only:tag");
  assert.equal(out.image, "only:tag");
  assert.equal(out.image_digest, null);
});
