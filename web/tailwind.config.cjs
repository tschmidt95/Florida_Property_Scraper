/** @type {import('tailwindcss').Config} */
module.exports = {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        cre: {
          bg: 'rgb(var(--cre-bg) / <alpha-value>)',
          surface: 'rgb(var(--cre-surface) / <alpha-value>)',
          primary: 'rgb(var(--cre-primary) / <alpha-value>)',
          accent: 'rgb(var(--cre-accent) / <alpha-value>)',
          text: 'rgb(var(--cre-text) / <alpha-value>)',
          muted: 'rgb(var(--cre-muted) / <alpha-value>)',
          border: 'rgb(var(--cre-border) / <alpha-value>)',
        },
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        serif: ['Playfair Display', 'ui-serif', 'Georgia', 'serif'],
      },
      borderRadius: {
        xl: '0.875rem',
      },
      boxShadow: {
        panel: '0 10px 30px rgba(0,0,0,0.12)',
      },
    },
  },
  plugins: [],
};
