'use strict';
/**
 * Name filter shared (by design) with the app: lib/core/name_filter.dart is
 * generated from blocklist.json and uses the very same steps, so a name the app
 * accepts is judged identically here. The server is the one that counts, because
 * a modified app could skip the app-side check.
 */
const path = require('path');
const LISTS = require(path.join(__dirname, 'blocklist.json'));

const FOLD = {
  à: 'a', â: 'a', ä: 'a', á: 'a', ã: 'a', å: 'a',
  é: 'e', è: 'e', ê: 'e', ë: 'e',
  í: 'i', ì: 'i', î: 'i', ï: 'i',
  ó: 'o', ò: 'o', ô: 'o', ö: 'o', õ: 'o',
  ú: 'u', ù: 'u', û: 'u', ü: 'u',
  ç: 'c', ñ: 'n', ÿ: 'y', œ: 'oe', æ: 'ae',
  'أ': 'ا', 'إ': 'ا', 'آ': 'ا', 'ٱ': 'ا', 'ى': 'ي', 'ة': 'ه', 'ؤ': 'و', 'ئ': 'ي',
  '0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's', '7': 't', '@': 'a', $: 's', '!': 'i',
};

function isLetter(cp) {
  return (cp >= 97 && cp <= 122) || (cp >= 0x621 && cp <= 0x63a) || (cp >= 0x641 && cp <= 0x64a);
}

/** letters only, folded, and runs of 3+ equal letters shortened to one. */
function norm(s) {
  const folded = [];
  for (const ch of String(s).toLowerCase()) {
    const c = Object.prototype.hasOwnProperty.call(FOLD, ch) ? FOLD[ch] : ch;
    for (const d of c) if (isLetter(d.codePointAt(0))) folded.push(d);
  }
  const out = [];
  let i = 0;
  while (i < folded.length) {
    let j = i;
    while (j < folded.length && folded[j] === folded[i]) j++;
    const run = j - i;
    if (run >= 3) out.push(folded[i]);
    else for (let k = 0; k < run; k++) out.push(folded[i]);
    i = j;
  }
  return out.join('');
}

const SUBSTR = LISTS.substr.map(norm);
const TOKEN = new Set(LISTS.token.map(norm));

const SPAM_RE = /(https?:|www\.|\.(com|net|org|ru|xyz|info|me|io|ly|tk)\b|t\.me|wa\.me|@[a-z0-9_]{3,})/i;
const PHONE_RE = /[0-9٠-٩](?:[^0-9٠-٩]*[0-9٠-٩]){6}/;
const SPLIT_RE = /[^\p{L}\p{N}@$!]+/u;

/** True when [name] may be shown to other players. */
function isNameAllowed(name) {
  const raw = String(name || '');
  if (SPAM_RE.test(raw) || PHONE_RE.test(raw)) return false;
  const full = norm(raw);
  if (TOKEN.has(full)) return false;
  for (const w of SUBSTR) if (w && full.includes(w)) return false;
  for (const t of raw.split(SPLIT_RE)) {
    if (!t) continue;
    if (TOKEN.has(norm(t))) return false;
  }
  return true;
}

module.exports = { isNameAllowed, norm };
