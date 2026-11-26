import { describe, expect, it } from 'vitest';
import { buildAllowedOrigins, isAllowedOrigin, normalizeOrigin, safeReturnTo } from './origin.js';

describe('origin helpers', () => {
  const allowed = buildAllowedOrigins(['http://localhost:5173/', 'https://app.example.com'], 'http://localhost:3000/app');

  it('reduces URLs to scheme://host[:port]', () => {
    expect(normalizeOrigin('http://localhost:5173/some/path?x=1')).toBe('http://localhost:5173');
    expect(normalizeOrigin('not a url')).toBeNull();
    expect(normalizeOrigin('null')).toBeNull();
  });

  it('includes the app base URL in the allow-list', () => {
    expect(allowed.has('http://localhost:3000')).toBe(true);
  });

  it.each([
    ['http://localhost:5173', true],
    ['https://app.example.com', true],
    ['http://localhost:3000', true],
    ['https://evil.example', false],
    ['http://localhost:5174', false], // same host, different port
    ['https://app.example.com.evil.example', false], // look-alike suffix
    ['http://app.example.com', false], // different scheme
    ['null', false],
    ['', false],
    [undefined, false],
  ])('isAllowedOrigin(%j) -> %s', (origin, expected) => {
    expect(isAllowedOrigin(origin, allowed)).toBe(expected);
  });

  it.each([
    ['/map', '/map'],
    ['/a?b=c', '/a?b=c'],
    ['https://evil.example', '/'],
    ['//evil.example', '/'],
    ['/\\evil.example', '/'],
    ['javascript:alert(1)', '/'],
    ['map', '/'],
    [undefined, '/'],
    [42, '/'],
  ])('safeReturnTo(%j) -> %s', (input, expected) => {
    expect(safeReturnTo(input)).toBe(expected);
  });
});
