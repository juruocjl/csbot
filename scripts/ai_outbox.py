"""Inspect/reconcile uncertain sends. Never sends any QQ message."""
import argparse
from pathlib import Path
import sqlite3


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state',type=Path,required=True)
    parser.add_argument('--id',help='Exact outbound UUID to reconcile')
    actions=parser.add_mutually_exclusive_group()
    actions.add_argument('--confirmed-mid',type=int,help='Administrator verified this send in QQ; archive that message ID')
    actions.add_argument('--confirmed-not-sent',action='store_true',help='Administrator verified delivery did not happen; release payload')
    args=parser.parse_args()
    with sqlite3.connect(args.state.resolve().as_uri()+'?mode=rw',uri=True,timeout=10) as db:
        if args.confirmed_mid is not None or args.confirmed_not_sent:
            if not args.id: parser.error('--id required for reconciliation')
            row=db.execute('SELECT status FROM outbound WHERE id=?',(args.id,)).fetchone()
            if row is None or row[0]!='unknown': raise ValueError('only an unknown send can be reconciled')
            if args.confirmed_mid is not None:
                db.execute("UPDATE outbound SET status='sent',message_id=?,error=NULL WHERE id=?",(args.confirmed_mid,args.id))
            else:
                db.execute("UPDATE outbound SET status='failed',payload='[]',error='AdminConfirmedNotSent' WHERE id=?",(args.id,))
            print('Reconciled; no platform message sent')
        else:
            for row in db.execute("SELECT id,group_id,created_at,status,message_id,error,length(CAST(payload AS BLOB)) FROM outbound WHERE status IN ('pending','sent','unknown') ORDER BY created_at LIMIT 100"):
                print(row)  # Metadata only; never image bytes or message contents.


if __name__=='__main__':main()
