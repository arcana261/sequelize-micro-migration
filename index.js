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
  /**
   * @desc create a new instance of SequelizeMicroMigration
   * @param {Sequelize} sequelize - instance to sequelize
   * @param {string} application - application or module name
   * @param {string} migrationDir - location of migration folder
   */
  constructor(sequelize, application, migrationDir) {
    this._metaDb = (new metaDb.MetaDB(sequelize)).prefix(
      `migration:${application}:`);
    this._versionDb = this._metaDb.prefix('version:');
    this._sequelize = sequelize;
    this._versions = null;
    this._migrationDir = migrationDir;
    this._current = null;
    this._currentVersionsList = null;
    this._application = application;
  }

  /**
   * @desc return application name
   * @return {string} - application name
   */
  get application() {
    return this._application;
  }

  /**
   * @desc is used to override fs module. used for test cases
   * @param {*} newFs - new fs module mock
   * @private
   */
  static _overrideFs(newFs) {
    fs = newFs;
  }

  /**
   * @desc is used to set "require" method for migrations.
   * used in test-cases to provide mocking functionality
   * @param {function} newRequire - new mock require function
   * @private
   */
  static _overrideRequire(newRequire) {
    _require = newRequire;
  }

  /**
   * @desc sort versions found in migration folder
   * @param {Array.<string>} versions - versions found in migration folder
   * @return {Array.<string>} - sorted set of migrations
   */
  sort(versions) {
    return iterable.from(versions)
      .select(x => ({
        path: x,
        version: x.split('-', 2)[0]
      }))
      .orderBy('version')
      .select(x => x.path)
      .toArray();
  }

  /**
   * @desc ensures that versions are loaded and cached from migration folder
   * @return {Promise.<Array.<string> >} - resolves when cache is set
   */
  versions() {
    if (type.isNull(this._versions)) {
      return fs.readdir(this._migrationDir)
        .then(files => {
          this._versions = this.sort(
            files.map(x => path.basename(x, path.extname(x))));
          return Promise.resolve(this._versions);
        });
    }

    return Promise.resolve(this._versions);
  }

  /**
   * @desc returns current database version
   * @return {Promise.<string>} - current database version
   */
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

  /**
   * @desc set last version on the database
   * @param {string} version - current latest version
   * @return {Promise} - resolves when version is set
   * @private
   */
  _setCurrent(version) {
    return this._metaDb.put('last', version);
  }

  /**
   * @desc sets that a version migration script is executed already
   * @param {string} version - migration script that is executed
   * @return {Promise} - resolves when mark is set
   * @private
   */
  _putVersion(version) {
    return this._versionDb.put(version, true);
  }

  /**
   * @desc unmarks a migration script due to "down" command
   * @param {string} version - migration script that is degraded
   * @return {Promise} - resolves when mark is unset
   * @private
   */
  _deleteVersion(version) {
    return this._versionDb.delete(version);
  }

  /**
   * @desc returns sorted array of database migrations currently up
   * in working database
   * @return {Promise.<Array.<string> >} - list of currently up migrations
   */
  currentVersions() {
    if (!type.isNull(this._currentVersionsList)) {
      return Promise.resolve(this._currentVersionsList);
    }

    return this._versionDb.all().then(all => {
      this._currentVersionsList = this.sort(all.map(x => x.key));
      return Promise.resolve(this._currentVersionsList);
    });
  }

  /**
   * @desc clears local cache of fetched data
   * @private
   */
  _clearCache() {
    this._versions = null;
    this._current = null;
    this._currentVersionsList = null;
  }

  /**
   * @desc lists migration actions necessary to travel to selected version
   * @param {number|string} [to] - target version. number can be used to
   * take migration steps
   * @return {Promise.<Array.<Array.<string> > >} - migration steps. first
   * item shows target version and second item shows action necessary, either
   * "up" or "down".
   */
  listUp(to) {
    const self = this;

    return task.spawn(function * task() {
      yield self.versions();
      const result = [];
      const currentVersionsArray = Array.from(yield self.currentVersions());
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

  /**
   * @desc lists migration actions necessary to travel down to selected version
   * @param {number|string} [to] - target version. number can be used to
   * take migration steps
   * @return {Promise.<Array.<Array.<string> > >} - migration steps. first
   * item shows target version and second item shows action necessary, either
   * "up" or "down".
   */
  listDown(to) {
    const self = this;

    return task.spawn(function * task() {
      yield self.versions();
      const currentVersionsArray = yield self.currentVersions();
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

  /**
   * @desc executes a single migration step
   * @param {Array.<string>} item - a single item. first item shows target
   * version and second item shows action to take, either "up" or "down".
   * @return {Promise} - resolves when migration is done
   */
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
          yield self.versions();
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

  /**
   * @desc executes all migration commands
   * @param {Array.<Array.<string> >} list - first string shows migration
   * version, second one shows action in form of "up" or "down".
   * @return {Promise} - resolves when migration is done
   * @private
   */
  _executeAll(list) {
    return iterable.async(list).each(x => this.execute(x));
  }

  /**
   * @desc travells database up to target version
   * @param {number|string} [to] - target version. number can be used to
   * take migration steps
   * @param {boolean=} force - if set to true, will take downgrade actions
   * as well.
   * @return {Promise} - resolves when migration is done
   */
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

  /**
   * @desc travells database down to target version
   * @param {number|string} [to] - target version. number can be used to
   * take migration steps
   * @return {Promise} - resolves when migration is done
   */
  down(to) {
    return this.listDown(to).then(list => this._executeAll(list));
  }

  /**
   * @desc specifies whether migration is necessary
   * @return {Promise.<boolean>} - true if migration is necessary
   */
  requiresMigration() {
    return this.listUp().then(list => Promise.resolve(list.length > 0));
  }
}

module.exports = SequelizeMicroMigration;
