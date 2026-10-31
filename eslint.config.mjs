import js from '@eslint/js';
import react from 'eslint-plugin-react';
import tseslint from 'typescript-eslint';

const blocks = ['if', 'for', 'while', 'do', 'switch', 'try'];

export default tseslint.config(
  { ignores: ['**/dist/**', '**/node_modules/**'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    plugins: { react },
    settings: { react: { version: 'detect' } },
    rules: {
      'padding-line-between-statements': [
        'warn',
        { blankLine: 'always', prev: '*', next: 'return' },
        { blankLine: 'always', prev: '*', next: blocks },
        { blankLine: 'always', prev: blocks, next: '*' },
        { blankLine: 'always', prev: ['const', 'let', 'var'], next: '*' },
        { blankLine: 'any', prev: ['const', 'let', 'var'], next: ['const', 'let', 'var'] },
        { blankLine: 'always', prev: '*', next: ['function', 'class', 'export'] },
        { blankLine: 'always', prev: ['function', 'class', 'export'], next: '*' },
        { blankLine: 'always', prev: 'import', next: '*' },
        { blankLine: 'any', prev: 'import', next: 'import' },
      ],
      'no-inline-comments': 'warn',
      'react/jsx-no-comment-textnodes': 'error',
      curly: ['warn', 'multi-line'],
      'no-console': ['warn', { allow: ['warn', 'error'] }],
      'prefer-const': 'warn',
      'no-var': 'error',
    },
  }
);
