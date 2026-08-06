'use strict';

const path = require('path');

module.exports = {
  apps: [{
    name: 'dump-sniper',
    cwd: path.resolve(__dirname, '..'),
    script: 'src/index-flow.js',
    watch: false,
    autorestart: true,
    restart_delay: 5000,
    exp_backoff_restart_delay: 250,
    min_uptime: '30s',
    max_restarts: 10,
    kill_timeout: 15000,
    max_memory_restart: '1500M',
    env: {
      NODE_ENV: 'production',
    },
  }],
};
