export const FALLBACK_REQUEST_CONTEXT = (request: string): string =>
	`A listener requested: "${request}". Interpret it creatively without copying copyrighted lyrics or imitating a living artist exactly.`;

export const FALLBACK_NO_REQUEST_CONTEXT = "No listener request is active. Invent a vivid left-field song concept fit for a strange internet radio station.";

export const FALLBACK_PROMPT_SUFFIX = "Make a polished, original 2-3 minute song with a strong hook, clear genre fusion, specific instrumentation, vocal direction, production texture, rhythmic motion, and emotional arc. Include an ear-catching intro, a memorable chorus, a dynamic bridge, and a satisfying outro. Avoid direct artist imitation and avoid quoting existing songs.";

export const PROMPT_UNIQUENESS_SUFFIX = "avoid the previous arrangement shape entirely and use a new sonic premise, tempo feel, instrumentation palette, and lyrical point of view.";

export const OVERUSED_NOUN_RETIREMENT_PREFIX = "Overused noun motifs to retire: these recur across recent titles, prompts, lyrics, or visual directions, so do not reuse them as imagery, characters, premises, visual motifs, or title words:";

export const OVERUSED_NOUN_DRAFT_REPAIR_PREFIX = "The draft reused over-retired station noun imagery before music generation:";

export const OVERUSED_NOUN_DRAFT_REPAIR_SUFFIX = "Rewrite with a fresh premise, title language, concrete imagery, and lyric setting while preserving the listener request only if it explicitly requires one of those words.";
