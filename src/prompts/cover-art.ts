import type { OverusedNounTerm } from "../lib";

export const COVER_PROMPT_PREFIX = "Square pictorial music image v4.";

export const COVER_NEGATIVE_PROMPT = "text, typography, letters, words, captions, signatures, watermarks, logos, labels, posters, cassettes, devices, UI, low quality, blurry, jpeg artifacts";

export const COVER_ART_STATIC_SUFFIX = "Make the image feel grounded in this specific song, not generic genre art. Focus on scene, color, lighting, texture, characters, landscape, architecture, abstract pattern, and motion. Use blank unmarked surfaces and purely pictorial shapes. Avoid devices, cassettes, posters, signage, labels, logos, watermarks, letters, numerals, and readable symbols. Bold editorial feeling, tactile texture, striking central composition, rich color contrast, layered depth, handmade detail, frame filled edge-to-edge with continuous visual detail.";

export type CoverArtPromptContext = {
	genre: string;
	mood: string;
	tags: string;
	anchors: string;
	direction: string;
	overusedNouns: ReadonlyArray<OverusedNounTerm>;
};

export function coverArtFullPrompt(ctx: CoverArtPromptContext): string {
	const retired = ctx.overusedNouns.length > 0
		? ` Avoid recurring station visual motifs and trope imagery associated with: ${ctx.overusedNouns.map((entry) => entry.word).join(", ")}.`
		: "";
	return `${COVER_PROMPT_PREFIX} ${ctx.genre}, ${ctx.mood}, ${ctx.tags}. ${ctx.anchors} ${ctx.direction ? `Visual direction: ${ctx.direction}.` : ""}${retired} ${COVER_ART_STATIC_SUFFIX}`.replace(/\s+/g, " ").trim();
}

export function coverNegativePromptWithRetired(overusedNouns: ReadonlyArray<OverusedNounTerm> = []): string {
	if (overusedNouns.length === 0) return COVER_NEGATIVE_PROMPT;
	return `${COVER_NEGATIVE_PROMPT}, repeated station motifs, recycled trope imagery, ${overusedNouns.map((entry) => entry.word).join(", ")}`;
}
