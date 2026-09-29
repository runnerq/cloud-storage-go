from pathlib import Path
import re
import os
root=Path(__file__).resolve().parents[3]
# Override the sibling checkouts, e.g. to generate from a clean SDK worktree.
sdk=Path(os.environ.get('RUNNERQ_GO_SDK',root/'runnerq-go-sdk'))
cloud=Path(os.environ.get('RUNNERQ_CLOUD',root/'runnerq-cloud'))
# storagetest.Reader is not part of storage.Storage, but the conformance suite
# reads through it, so the adapter serves those reads too.
src='\n'.join((sdk/'storage'/f).read_text() for f in ['storage.go','recovery.go','storagetest/storagetest.go']).replace('storage.','')
types=set(re.findall(r'^type (\w+)',src,re.M))
interfaces=['ResultStorage','QueueStorage','Reader','BatchQueueStorage','AttemptLeaseStorage','CheckpointStorage','SpawnStorage','DependencyStorage']
def qualify(s):
    return re.sub(r'\b[A-Za-z_]\w*\b',lambda m:'storage.'+m[0] if m[0] in types and m[0] not in ('Reader','Harness','suite') else m[0],s)
methods={}
reader=set()
for interface in interfaces:
    block=re.search(r'type '+interface+r' interface \{(.*?)\n\}',src,re.S)[1]
    for line in block.splitlines():
        m=re.match(r'\s*(\w+)\(ctx context.Context(?:, (.*?))?\) (.+)$',line)
        if not m: continue
        name,args,returns=m.groups(); params=[]; pending=[]
        for part in (args or '').split(', '):
            if not part:continue
            x=part.split(' ',1)
            if len(x)==1:pending.append(x[0]);continue
            param,typ=x
            params.extend((p,qualify(typ)) for p in pending+[param]);pending=[]
        returns=returns.strip('()').split(', ')
        methods[name]=(params,[qualify(t) for t in returns])
        if interface=='Reader': reader.add(name)
imports='''import ("context"; "encoding/json"; "time"; "github.com/google/uuid"; "github.com/alob-mtc/runnerq-go/storage")\n'''
wire=['// Code generated from the RunnerQ storage contract; DO NOT EDIT.\npackage protocol\n',imports.replace('"context"; ','' )]
client=['// Code generated from the RunnerQ storage contract; DO NOT EDIT.\npackage backend\n',imports, 'import "github.com/runnerq/cloud-storage-go/protocol"\n']
server=['// Code generated from the RunnerQ storage contract; DO NOT EDIT.\npackage rpc\n', 'import ("context"; "encoding/json"; "time"; "github.com/google/uuid"; "github.com/alob-mtc/runnerq-go/storage"; "github.com/runnerq/cloud-storage-go/protocol")\n','type Backend interface { storage.Storage; storage.BatchQueueStorage; storage.AttemptLeaseStorage; storage.CheckpointStorage; storage.SpawnStorage; storage.DependencyStorage; storage.ResultWaiter; reader }\n','func dispatch(ctx context.Context,b Backend,method string,body json.RawMessage)(any,error){ switch method {\n']
reads=[]
for name,(params,ret) in methods.items():
    if name not in reader: continue
    rettype=ret[0] if len(ret)==1 else '('+', '.join(ret)+')'
    reads.append(f"{name}(ctx context.Context{''.join(', '+p+' '+t for p,t in params)}) {rettype}\n")
server.insert(2,'// reader is the read side the conformance suite checks through (storagetest.Reader).\ntype reader interface {\n'+''.join(reads)+'}\n')
for name,(params,ret) in methods.items():
    fields=';'.join(p[0].upper()+p[1:]+' '+t+' `json:"'+p+'"`' for p,t in params)
    wire.append(f'type {name}Args struct {{{fields}}}\n')
    args=','.join(p[0].upper()+p[1:]+':'+p for p,t in params)
    signature=', '.join(p+' '+t for p,t in params)
    signature=(','+signature) if signature else ''
    rettype=ret[0] if len(ret)==1 else '('+', '.join(ret)+')'
    client.append(f'func(b *CloudBackend) {name}(ctx context.Context{signature}) {rettype} {{\n')
    if len(ret)==1:client.append(f'return b.call(ctx,"{name}",protocol.{name}Args{{{args}}},nil)\n}}\n')
    else:client.append(f'var out {ret[0]}; err:=b.call(ctx,"{name}",protocol.{name}Args{{{args}}},&out); return out,err\n}}\n')
    server.append(f'case "{name}": var a protocol.{name}Args; if err:=decode(body,&a);err!=nil{{return nil,err}}\n')
    # Validate request bounds before entering SQL and reject nil activity pointers.
    for p,t in params:
        f='a.'+p[0].upper()+p[1:]
        if t=='*storage.QueuedActivity':server.append(f'if {f}==nil{{return nil,invalid("{p} is required")}}\n')
        if t in ('storage.QueuedActivity','*storage.QueuedActivity'):server.append(f'if err:=validateActivity({"&" if t=="storage.QueuedActivity" else ""}{f});err!=nil{{return nil,err}}\n')
        if t=='time.Duration':server.append(f'if {f}<0 || {f}>maxDuration{{return nil,invalid("{p} out of range")}}\n')
        if p in ('limit','batchSize'):server.append(f'if {f}<{0 if p=="limit" else 1} || {f}>1000{{return nil,invalid("{p} must be between {0 if p=="limit" else 1} and 1000")}}\n')
        if p=='offset':server.append(f'if {f}<0{{return nil,invalid("offset must be non-negative")}}\n')
        if p in ('workerID','workerIDPrefix'):server.append(f'if len({f})==0 || len({f})>512{{return nil,invalid("worker token is required and must be <=512 bytes")}}\n')
        if t=='storage.RetentionPolicy':server.append(f'if {f}.Completed<0 || {f}.Failed<0{{return nil,invalid("negative retention")}}\n')
    call=f'b.{name}(ctx'+''.join(',a.'+p[0].upper()+p[1:] for p,t in params)+')'
    if name in ('Dequeue','DequeueBatch'):call=call.replace('a.Timeout','min(a.Timeout,25*time.Second)')
    server.append(('return nil,' if len(ret)==1 else 'return ')+call+'\n')
server.append('default: return nil,&storage.StorageError{Kind:storage.ErrUnsupported,Message:"unsupported storage operation"}\n}}\n')
(root/'cloud-storage-sdk/go/protocol/operations.go').write_text(''.join(wire))
(root/'cloud-storage-sdk/go/operations.go').write_text(''.join(client))
(cloud/'dataplane/internal/rpc/operations.go').write_text(''.join(server))
print('Generated',len(methods),'operations')

import subprocess
subprocess.run(['gofmt','-w',str(root/'cloud-storage-sdk/go/protocol/operations.go'),str(root/'cloud-storage-sdk/go/operations.go'),str(cloud/'dataplane/internal/rpc/operations.go')],check=True)
