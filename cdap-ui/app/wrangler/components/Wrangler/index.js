/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React, { Component, PropTypes } from 'react';
import Papa from 'papaparse';
import WrangleData from 'wrangler/components/Wrangler/WrangleData';
import WranglerActions from 'wrangler/components/Wrangler/Store/WranglerActions';
import WranglerStore from 'wrangler/components/Wrangler/Store/WranglerStore';
import shortid from 'shortid';
import Dropzone from 'react-dropzone';
import {convertHistoryToDml} from 'wrangler/components/Wrangler/dml-converter';
import Explore from 'wrangler/components/Explore';
import T from 'i18n-react';

require('./Wrangler.less');

/**
 * 3 Steps for any transforms:
 *    1. Format the data
 *    2. Handle the ordering of Headers
 *    3. Handle the inferred type of the column
 **/
export default class Wrangler extends Component {
  constructor(props) {
    super(props);

    this.state = {
      textarea: false,
      loading: false,
      header: false,
      skipEmptyLines: false,
      delimiter: '',
      wranglerInput: '',
      isDataSet: false,
      file: '',
      errors: []
    };

    this.handleSetHeaders = this.handleSetHeaders.bind(this);
    this.handleSetSkipEmptyLines = this.handleSetSkipEmptyLines.bind(this);
    this.setDelimiter = this.setDelimiter.bind(this);
    this.wrangle = this.wrangle.bind(this);
    this.handleData = this.handleData.bind(this);
    this.handleTextInput = this.handleTextInput.bind(this);
    this.onPlusButtonClick = this.onPlusButtonClick.bind(this);
    this.onWrangleClick = this.onWrangleClick.bind(this);
    this.getWranglerOutput = this.getWranglerOutput.bind(this);
    this.onTextInputBlur = this.onTextInputBlur.bind(this);
  }

  getChildContext() {
    return {source: this.props.source};
  }

  componentWillUnmount() {
    WranglerStore.dispatch({
      type: WranglerActions.reset
    });
  }
  onWrangleClick() {
    let input = this.state.wranglerInput;

    if (this.state.file) {
      input = this.state.file;
    }

    this.wrangle(input, this.state.delimiter, this.state.header, this.state.skipEmptyLines);
  }

  wrangle(input, delimiter, header, skipEmptyLines) {
    this.setState({loading: true});

    let papaConfig = {
      header: header,
      skipEmptyLines: skipEmptyLines,
      complete: this.handleData
    };

    if (delimiter) {
      papaConfig.delimiter = delimiter;
    }

    Papa.parse(input, papaConfig);
  }

  onPlusButtonClick() {
    this.setState({textarea: true});
  }

  handleData(papa) {
    if (papa.errors.length > 0) {
      this.setState({errors: papa.errors, loading: false});
      return;
    }

    let formattedData;
    if (Array.isArray(papa.data[0])) {
      // Get length of the longest column
      let numColumns = papa.data.reduce((prev, curr) => {
        return prev > curr.length ? prev : curr.length;
      }, 0);

      formattedData = papa.data.map((row) => {
        let obj = {};
        for (let i = 0; i < numColumns; i++) {
          let key = `column${i+1}`;
          let value = row[i];

          obj[key] = !value ? '' : value;
        }

        return obj;
      });
    } else {
      formattedData = papa.data;
    }

    WranglerStore.dispatch({
      type: WranglerActions.setData,
      payload: {
        data: formattedData,
      }
    });

    this.setState({
      isDataSet: true,
      loading: false,
      delimiter: papa.meta.delimiter
    });
  }

  setDelimiter(e) {
    this.setState({delimiter: e.target.value});
  }

  handleSetHeaders() {
    this.setState({header: !this.state.header});
  }
  handleSetSkipEmptyLines() {
    this.setState({skipEmptyLines: !this.state.skipEmptyLines});
  }
  handleTextInput(e) {
    this.setState({wranglerInput: e.target.value});
  }

  renderErrors() {
    if (this.state.errors.length === 0) {
      return null;
    }

    return (
      <div className="wrangler-error-container">
        <h4 className="error text-center">Errors:</h4>
        <table className="table table-bordered error-table">
          <thead>
            <tr>
              <th>{T.translate('features.Wrangler.InputScreen.ErrorTable.row')}</th>
              <th>{T.translate('features.Wrangler.InputScreen.ErrorTable.error')}</th>
            </tr>
          </thead>
          <tbody>
            {
              this.state.errors.map((error) => {
                return (
                  <tr key={shortid.generate()}>
                    <td>{error.row}</td>
                    <td>{error.message}</td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );

  }

  preventPropagation (e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
  }

  renderWranglerCopyPaste() {
    if (!this.state.textarea || this.state.file.name) {
      return (
        <div
          className="wrangler-plus-button text-center"
          onClick={this.onPlusButtonClick}
        >
          <div
            className="dropzone-container"
            onClick={(e) => this.preventPropagation(e)}
          >
            <Dropzone
              activeClassName="wrangler-file-drag-container"
              className="wrangler-file-drop-container"
              onDrop={(e) => this.setState({file: e[0], textarea: false})}
            >
              {
                this.state.file.name && this.state.file.name.length ?
                  null
                :
                  (
                    <div className="plus-button">
                      <i className="fa fa-plus-circle"></i>
                    </div>
                  )
              }
            </Dropzone>
          </div>

          <div className="plus-button-helper-text">
            {
              this.state.file.name && this.state.file.name.length ?
              (<h4>{this.state.file.name}</h4>)
                :
              (
                <div>
                  <h4>
                    {T.translate('features.Wrangler.InputScreen.HelperText.click')}
                    <span className="fa fa-plus-circle" />
                    {T.translate('features.Wrangler.InputScreen.HelperText.upload')}
                  </h4>
                  <h5>{T.translate('features.Wrangler.InputScreen.HelperText.or')}</h5>
                  <h4>{T.translate('features.Wrangler.InputScreen.HelperText.paste')}</h4>
                </div>
              )
            }
          </div>
        </div>
      );
    }

    return (
      <textarea
        className="form-control"
        onChange={this.handleTextInput}
        autoFocus={true}
        onBlur={this.onTextInputBlur}
      />
    );
  }

  onTextInputBlur() {
    if (this.state.wranglerInput) { return; }

    this.setState({textarea: false});
  }

  getWranglerOutput() {
    let state = WranglerStore.getState().wrangler;

    // prepare specifications
    let dml = convertHistoryToDml(state.history.slice(0, state.historyLocation));

    let setFormat = `set format csv ${this.state.delimiter} ${this.state.skipEmptyLines}`;
    let initialColumns = state.initialHeaders.join(',');
    let setColumns = `set columns ${initialColumns}`;

    let spec = [setFormat, setColumns].concat(dml).join('\n');


    // prepare schema
    let fields = [];

    state.headersList.forEach((column) => {
      let hasError = state.errors[column] && state.errors[column].count > 0;

      fields.push({
        name: column,
        type: hasError ? [state.columnTypes[column], 'null'] : state.columnTypes[column]
      });
    });

    let schema = {
      name: 'etlSchemaBody',
      type: 'record',
      fields
    };

    let properties = {
      specification: spec,
      schema: JSON.stringify(schema)
    };

    if (typeof this.props.hydrator === 'function') {
      this.props.hydrator(properties);
    }

    return properties;
  }

  renderWranglerInputBox() {
    if (this.state.isDataSet) {
      return null;
    }

    if (this.state.loading) {
      return (
        <div className="loading text-center">
          <div>
            <span className="fa fa-spinner fa-spin"></span>
          </div>
          <h3>{T.translate('features.Wrangler.parsing')}</h3>
        </div>
      );
    }

    return (
      <div className="wrangler-input-container">
        <div className="wrangler-title"></div>

        <Explore wrangle={this.wrangle} />

        <div className="wrangler-copy-paste">
          {this.renderWranglerCopyPaste()}

          <div className="parse-options">
            <form className="form-inline">
              <div className="delimiter">
                {/* delimiter */}
                <input
                  type="text"
                  className="form-control"
                  placeholder={T.translate('features.Wrangler.InputScreen.Options.delimiter')}
                  onChange={this.setDelimiter}
                />
              </div>

              <hr/>

              <div className="checkbox">
                {/* header */}
                <label>
                  <input type="checkbox"
                    onChange={this.handleSetHeaders}
                    checked={this.state.headers}
                  /> {T.translate('features.Wrangler.InputScreen.Options.firstLineAsColumns')}
                </label>
              </div>

              <div className="checkbox">
                {/* skipEmptyLines */}
                <label>
                  <input type="checkbox"
                    onChange={this.handleSetSkipEmptyLines}
                  /> {T.translate('features.Wrangler.InputScreen.Options.skipEmptyLines')}
                </label>
              </div>
            </form>
          </div>

        </div>

        <br/>

        <div className="text-center">
          <button
            className="btn btn-wrangler wrangle-button"
            onClick={this.onWrangleClick}
          >
            {T.translate('features.Wrangler.wrangleButton')}
          </button>
        </div>

        {this.renderErrors()}

      </div>
    );
  }

  render() {
    return (
      <div className="wrangler-container">

        {this.renderWranglerInputBox()}

        {
          this.state.isDataSet ?
            <WrangleData
              onHydratorApply={this.getWranglerOutput}
            />
          :
            null
        }

      </div>
    );
  }
}

Wrangler.propTypes = {
  source: PropTypes.oneOf(['wrangler', 'hydrator']),
  hydrator: PropTypes.any
};

Wrangler.childContextTypes = {
  source: PropTypes.oneOf(['wrangler', 'hydrator'])
};
