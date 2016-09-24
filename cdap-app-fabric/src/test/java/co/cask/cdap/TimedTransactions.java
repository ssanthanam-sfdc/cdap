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

package co.cask.cdap;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import com.google.common.base.Throwables;
import org.apache.tephra.TransactionFailureException;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;

public class TimedTransactions {

  public static void testTimedTransactions(Transactional context) {
    try {
      context.execute(1, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          TimeUnit.MILLISECONDS.sleep(2000);
        }
      });
      Assert.fail("transaction should have timed out");
    } catch (TransactionFailureException e) {
      // expected
    }
    try {
      context.execute(5, new TxRunnable() {
        @Override
        public void run(DatasetContext context) throws Exception {
          TimeUnit.MILLISECONDS.sleep(100);
        }
      });
    } catch (TransactionFailureException e) {
      throw Throwables.propagate(e);
    }
  }
}
