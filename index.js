"use strict";

const metaDb = require('sequelize-db-meta');
let fs = require('mz/fs');
const type = require('xcane').type;
const task = require('xcane').task;
const path = require('path');
const iterable = require('xcane').iterable;
let _require = x => require(x);

/**
 * @desc provides sub-module based migration based on sequelize
 * @author Mohamad mehdi Kharatizadeh - m_kharatizadeh@yahoo.com
 */
class SequelizeMicroMigration {
  constructor(sequelize, application, migrationDir) {
    this._metaDb = (new metaDb.MetaDB(sequelize)).prefix(
      `migration:${application}:`);
    this._versionDb = this._metaDb.prefix('version:');
    this._sequelize = sequelize;
    this._versions = null;
    this._migrationDir = migrationDir;
    this._current = null;
    this._currentVersionsList = null;
  }

  static _overrideFs(newFs) {
    fs = newFs;
  }

  static _overrideRequire(newRequire) {
    _require = newRequire;
  }

  _sort(versions) {
    return iterable.from(versions)
      .select(x => ({
        path: x,
        version: x.split('-', 2)[0]
      }))
      .orderBy('version')
      .select(x => x.path)
      .toArray();
  }

  _ensure() {
    if (type.isNull(this._versions)) {
      return fs.readdir(this._migrationDir)
        .then(files => {
          this._versions = this._sort(
            files.map(x => path.basename(x, path.extname(x))));
          return Promise.resolve(this._versions);
        });
    }

    return Promise.resolve();
  }

  current() {
    if (!type.isNull(this._current)) {
      return Promise.resolve(this._current);
    }

    return this._metaDb.getOrDefault('last', '0')
      .then(current => {
        this._current = current;
        return Promise.resolve(current);
      });
  }

  _setCurrent(version) {
    return this._metaDb.put('last', version);
  }

  _putVersion(version) {
    return this._versionDb.put(version, true);
  }

  _deleteVersion(version) {
    return this._versionDb.delete(version);
  }

  _currentVersions() {
    if (!type.isNull(this._currentVersionsList)) {
      return Promise.resolve(this._currentVersionsList);
    }

    return this._versionDb.all().then(all => {
      this._currentVersionsList = this._sort(all.map(x => x.key));
      return Promise.resolve(this._currentVersionsList);
    });
  }

  _clearCache() {
    this._versions = null;
    this._current = null;
    this._currentVersionsList = null;
  }

  listUp(to) {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();
      const result = [];
      const currentVersionsArray = Array.from(yield self._currentVersions());
      const currentVersions = currentVersionsArray
        .reduce((prev, x, i) => Object.assign(prev, {
          [x]: i
        }), {});

      if (type.isNumber(to)) {
        if (currentVersionsArray.length > 0) {
          const index = self._versions.indexOf(
            currentVersionsArray[currentVersionsArray.length - 1]);

          if (index >= 0) {
            to = self._versions[index + to];
          } else {
            to = null;
          }
        } else {
          to = self._versions[0];
        }
      }

      for (let i = 0; i < self._versions.length; i++) {
        const version = self._versions[i];
        const j = currentVersions[version];

        if (i !== j) {
          for (let k = currentVersionsArray.length - 1; k >= i; k--) {
            const at = currentVersionsArray[k];
            result.push([at, 'down']);
            delete currentVersions[at];
            currentVersionsArray.pop();
          }

          result.push([version, 'up']);
        }

        if (!type.isOptional(to) && version === to) {
          break;
        }
      }

      return result;
    });
  }

  listDown(to) {
    const self = this;

    return task.spawn(function * task() {
      yield self._ensure();
      const currentVersionsArray = yield self._currentVersions();
      let result = yield self.listUp();

      if (type.isNumber(to)) {
        if (currentVersionsArray.length > 0) {
          const index = self._versions.indexOf(
            currentVersionsArray[currentVersionsArray.length - 1]);

          if (index >= 0) {
            to = self._versions[index - to];
          } else {
            to = null;
          }
        } else {
          to = self._versions[0];
        }
      }

      for (let i = self._versions.length - 1; i >= 0; i--) {
        const version = self._versions[i];

        if (!type.isOptional(to) && version === to) {
          break;
        }

        result.push([version, 'down']);
      }

      for (let i = 0; i < result.length; i++) {
        let count = 0;

        while ((i - count) >= 0 &&
          (i + count + 1) < result.length &&
          result[i - count][1] === 'up' &&
          result[i + count + 1][1] === 'down' &&
          result[i - count][0] === result[i + count + 1][0]) {
          count++;
        }

        if (count > 0) {
          result.splice(i - count + 1, count * 2);
          break;
        }
      }

      return result;
    });
  }

  execute(item) {
    const version = item[0];
    const act = item[1];
    const script = _require(path.join(this._migrationDir, version));
    const self = this;

    return this._sequelize.transaction(t =>
      task.spawn(function * task() {
        if (act === 'up') {
          yield script.up(
            self._sequelize.getQueryInterface(), self._sequelize);
          yield self._putVersion(version);
          yield self._setCurrent(version);
          self._clearCache();
        } else {
          yield script.down(
            self._sequelize.getQueryInterface(), self._sequelize);
          yield self._deleteVersion(version);
          yield self._ensure();
          const index = self._versions.indexOf(version);
          if (index < 1) {
            yield self._setCurrent('0');
          } else {
            yield self._setCurrent(self._versions[index - 1]);
          }
          self._clearCache();
        }
      }));
  }

  _executeAll(list) {
    return iterable.async(list).each(x => this.execute(x));
  }

  up(to, force) {
    return this.listUp(to).then(list => {
      if (!type.isBoolean(force)) {
        force = false;
      }

      if (!force && list.some(x => x[1] === 'down')) {
        return Promise.reject(new Error('migration might cause loss of data,' +
          'continue with force flag if necessary'));
      }

      return this._executeAll(list);
    });
  }

  down(to) {
    return this.listDown(to).then(list => this._executeAll(list));
  }

  requiresMigration() {
    return this.listUp().then(list => Promise.resolve(list.length > 0));
  }
}

module.exports = SequelizeMicroMigration;
