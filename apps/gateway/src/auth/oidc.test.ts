import { describe, expect, it } from 'vitest';
import { userFromClaims } from './oidc.js';

describe('userFromClaims', () => {
  it('uses sub as the id and prefers name, then preferred_username, then email', () => {
    expect(userFromClaims({ sub: 's1', name: 'Alice A', preferred_username: 'alice', email: 'a@x.com' })).toEqual({
      sub: 's1',
      displayName: 'Alice A',
      email: 'a@x.com',
    });
    expect(userFromClaims({ sub: 's1', preferred_username: 'alice' }).displayName).toBe('alice');
    expect(userFromClaims({ sub: 's1', email: 'a@x.com' }).displayName).toBe('a@x.com');
    expect(userFromClaims({ sub: 's1' }).displayName).toBe('s1');
  });

  it('ignores blank and non-string claims', () => {
    expect(userFromClaims({ sub: 's1', name: '   ', preferred_username: 7 }).displayName).toBe('s1');
  });

  it('truncates very long names to the contract limit of 80', () => {
    expect(userFromClaims({ sub: 's1', name: 'x'.repeat(200) }).displayName).toHaveLength(80);
  });

  it('drops an invalid email instead of blocking the login', () => {
    const user = userFromClaims({ sub: 's1', name: 'Alice', email: 'not-an-email' });
    expect(user).toEqual({ sub: 's1', displayName: 'Alice' });
  });
});
