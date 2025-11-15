--!strict

--@author BostonWhaIer
--@created: 10/28/2024
--@updated: 11/11/2024
--@desc: Chunking

--[[
Notes: 10/31/2024

This package is a wrapper around conventional datastores that provides a mechanism for efficient retrieval of large
amounts of data whilst attempting to minimize the amount of datastore requests being made. 

This was motivated by the DataStore request ratelimits that applications which presently have a request rate that exceeds
their alloted rate inevitably encounter. These ratelimits lead to detrimental negative results on program throughput and
user experience to name just two. 

Therefore this module is an attempt to provide not a solution considering there are some compromises and caveats associated
but to provide a better angle of attack on these problems then simply surrendering to the naive / brute force approach.
	
	Application of this module 
		This module was conceived of as a result of the problems I was encountering in my Degrees of Separation game which
		is a game that attempts to discover the shortest path of friend connections between any source and destination user 
		if possible. However due to the recent API ratelimits on the Players:GetFriendsAsync requests it led to needing to 
		use datastores which stored these request results which was somewhat effective at improving efficiency however
		is still nowhere near the speed of the search prior to the introduction of the more oppressive ratelimits 
		on the GetFriendsAsync request.
		
		For this reason I believe it is sensible to create a module that takes advantage of the 4MB datastore entry limit
		to store a maximal quantity friendship data (that is [userId] = list of friends) per entry. Which in theory will 
		lead to having to make less datastore requests. In order to still make this efficient some kind of encoding / 
		hashing algorithm which decides which chunk (datastore key) to assign a particular datum to will be necessary. 
		
		In conjunction with caching this module will hopefully prove of great importance to solving the aforementioned
		obstacles associated with the datastore request ratelimits.

		Additionally a future extension to this module could be the use of boatbombers module (freedumbstore) in order to 
		create additional datastores that attempt to make it seem like you are just interacting with one datastore. 
		
		Some other considerations I have for this module include using MemoryStores to coordinate updating local caches 
		between servers and also adding a timestamp for all data added incase I want to update (such as periodically
		checking if a users friendships have changed) 
		
	Alternatives
		Store the server sided cache of friends in a datastore at the end of a session and collectively load those at
		the beginning of each session
		
Notes: 11/11/2024

TODO: At some point we need to account for the size of the key in determining if the chunk has enough space

Notes 11/13/2024

TODO: At some point add a mechanism for dealing with entries that are simply too big to store in a chunk, i.e.
break it up into subchunks and then have some system for keeping track of the fragments of that chunk

Notes 4/8/2025

TODO: Speed up operations involving getting the size of the data in each chunk / entry through updating the chunk size as elements are inserted / removed.
]]

local DataStoreService = game:GetService("DataStoreService")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local HttpService = game:GetService("HttpService")

local Promise = require(ReplicatedStorage.Shared.Packages.Promise) 
--local Signal = require(ReplicatedStorage.Shared.Packages.Signal) [Probably don't need this for now] 
local TableUtil = require(ReplicatedStorage.Shared.Utility.TableUtil)

local CHUNK_UPDATE_INTERVAL = 30
local DEFAULT_SHELF_LIFE = 604800 -- 1 week 
local CHUNK_DATA_LIMIT = 3000000

local NOT_FOUND_TEST = true -- used to test situations in which the data is not found 

type ChunkConfig = {
	Name: string,
	Scope: string?, 
	IsTemporal: boolean?, 
	ShelfLife: number?
}

type ChunkProto = {
	__index: ChunkProto, 
	
	new: (ChunkConfig) -> Chunk,
	_loadChunk: (number) -> VirtualChunk, 
	_getCandidateChunk: (number) -> (VirtualChunk, number),
	
	-- fill in the rest of these later
}

type Chunk = typeof(setmetatable({} :: {
	_chunkDataStore: GlobalDataStore, 
	_virtualChunks: {VirtualChunk}, 
	_isTemporal: boolean, 
	_shelfLife: number
}, {} :: ChunkProto)) 

type VirtualChunk = {
	_savedContent: {}, -- what's in the datastore already 
	_virtualContent: {}, -- what needs to get put in the datastore
	
	_expiredKeys: {string}, 
	
	_lastUpdate: number, 

	_savedContentSize: number,
	_virtualContentSize: number,
	
	_isCapped: boolean, 
	
	IsLoaded: boolean,
} 

local Chunking = {} 
Chunking.__index = Chunking

-- add some functionality that replaces entries that are older then a certain date

function Chunking.new(config: ChunkConfig) 
	local self = setmetatable({
		_chunkDataStore = DataStoreService:GetDataStore(config.Name, config.Scope),
		_virtualChunks = {},
		_isTemporal = config.IsTemporal or false, 
		_shelfLife = config.ShelfLife or DEFAULT_SHELF_LIFE -- how long entry can last without updating
	}, Chunking) 
	
	return self
end


-- in the real implementation we will only update the chunnk every so often, we just update the cached table for the chunk and then save it at different times 
-- we will call these cache tables (virtual chunks) 

-- currently have this VERY cheesy update mechanism for chunks just as a proof of concept 

-- this whole module will eventually need to be reliant on some kind of datastore queuing system which will ensure that operations are carried out sequentially 
-- could also use some typa semaphore

-- learn more about how to deal with race conditions in the future online

-- again, i will need some datastore mechanism to prevent the race conditions that pop up when you are trying to access a chunk when it's currently loading 

function Chunking:_loadChunk(chunkId: number): VirtualChunk
	if self._virtualChunks[chunkId] then
		return self._virtualChunks[chunkId] 
	else		
		local newVirtualChunk: VirtualChunk = {
			_savedContent = {}, 
			_virtualContent = {}, 
			
			_expiredKeys = {}, 
			
			_lastUpdate = os.time(), 
			
			_savedContentSize = 0, 
			_virtualContentSize = 0, 
			
			_isCapped = false, 
			
			IsLoaded = false,
			--Loaded = Signal.new() 
		} 
		
		self._virtualChunks[chunkId] = newVirtualChunk
		
		local content = self._chunkDataStore:GetAsync(chunkId) or {}
		local contentSize: number = #HttpService:JSONEncode(content)
		
		newVirtualChunk._savedContent = content
		
		newVirtualChunk._isCapped = contentSize >= CHUNK_DATA_LIMIT
		newVirtualChunk._savedContentSize = contentSize
		
		newVirtualChunk.IsLoaded = true
		--newVirtualChunk.Loaded:Fire()
				
		return newVirtualChunk
	end
end

-- finds a chunk that is able to accommodate an amount of data the size of entrySize
function Chunking:_getCandidateChunk(entrySize): (VirtualChunk, number)
	local cId: number = 0 

	local chunk: VirtualChunk = self:_loadChunk(cId) 

	while chunk._isCapped or (CHUNK_DATA_LIMIT - (chunk._savedContentSize + chunk._virtualContentSize)) < entrySize do
		cId += 1
		chunk = self:_loadChunk(cId) 
	end
	
	return chunk, cId
end

function Chunking:_updateSavedContent(chunkId: number)
	local chunk = self:_loadChunk(chunkId)
	
	if not chunk then
		warn("Attempted to update a nonexistent chunk")
		return
	end
	
	chunk._lastUpdate = os.time()
	chunk.IsLoaded = false

	local content = {}
	
	self._chunkDataStore:UpdateAsync(tostring(chunkId), function(chunkData)
		chunkData = chunkData or {}

		-- what happens if two things in virtualcontent have the same key?
		for k, v in pairs(chunk._virtualContent) do
			if not chunkData[k] then
				chunkData[k] = v
			end
		end

		-- deal with expiredkeys 
		for k, v in ipairs(chunk._expiredKeys) do
			if chunkData[v] then
				chunkData[v] = nil
			else
				warn(string.format("Expired key %s not found in chunk%d", v, chunkId)) 
			end
		end
		
		print(string.format("REMOVED %d KEYS FROM CHUNK ID %d", #chunk._expiredKeys, chunkId)) 

		content = chunkData

		return chunkData 
	end)

	chunk._savedContent = content
	chunk._savedContentSize = #HttpService:JSONEncode(content)
	-- TODO: instead of JSONEncoding the entire content again (very expensive), should just
	-- see if simple addition is a viable solution to this (adding on the size of the jsonencoded data)

	-- need better savedcontentsize tracking

	--TODO: give the virtualcontent table weak metatable keys so if keys store a reference they can get garbage collected
	chunk._virtualContent = {} -- new reference
	chunk._virtualContentSize = 0

	chunk._expiredKeys = {}

	chunk.IsLoaded = true
	--chunk.Loaded:Fire()
end

-- want to clean this up later, not the cleanest code ever ngl 
function Chunking:_queueKeyRemoval(chunkId: number, key: string)
	local chunk: VirtualChunk = self:_loadChunk(chunkId)

	if not chunk then
		warn("Attempted to load a nonexistent chunk")
		return
	end
	
	if not table.find(chunk._expiredKeys, key) then
		table.insert(chunk._expiredKeys, key)
		
		if os.time() - chunk._lastUpdate >= CHUNK_UPDATE_INTERVAL then
			self:_updateSavedContent(chunkId)
		end
	else
		warn("Double remove attempted")
	end
end

function Chunking:_formatValue<T>(value: T): {Data: T, LastUpdate: number?}
	if self._isTemporal then
		return {
			Data = value,
			LastUpdate = os.time()
		}
	else
		return {
			Data = value
		} 
	end
end

function Chunking:_isValueExpired(value)
	if self._isTemporal then -- checks to see if we need to be worrying about that LastUpdate tag
		return value.LastUpdate == nil or (os.time() - value.LastUpdate) > self._shelfLife
	else
		return value.Data ~= nil -- checks if it's been formatted correctly recently, since we are using this {Data: a} format now, so every entry MUST have the data field
	end
end

function Chunking:_storeEntry(key: string, value)	
	if NOT_FOUND_TEST then
		warn("NOT FOUND TEST IS ENABLED, NOT STORED")
		return
	end	
	
	value = self:_formatValue(value)
	
	local entrySize: number = #HttpService:JSONEncode(value) 
	local candidate: VirtualChunk, chunkId: number = self:_getCandidateChunk(entrySize)
	
	-- TODO: dealing with entries that sneak in while it's updating? Could mess with the loop and data may get dropped. 
	-- have something that keeps track of whether or not the chunk is locked or something, and if it is put it in a list of data that needs to be saved
	-- eventually and have it just recall this function for every chunk
	if candidate.IsLoaded == false then
		warn("TRYING TO INSERT ENTRY WHILE CHUNK IS UPDATING")
	end
	candidate._virtualContentSize += entrySize
	candidate._virtualContent[key] = value
	
	if os.time() - candidate._lastUpdate >= CHUNK_UPDATE_INTERVAL then
		self:_updateSavedContent(chunkId)
	end
end

-- how shall we deal with versioning? I say remove from the chunk and store elsewhere when the time comes
-- don't really feel like fixing this code so this is a function so i can just use teh old syntax 

function Chunking:InsertEntry(key: string, value: any)
	return Promise.resolve(self:_storeEntry(key, value)) 
end

function Chunking:Get(key: string, chunkId: number?)
	chunkId = chunkId or 0

	return Promise.new(function(resolve, reject)
		if NOT_FOUND_TEST then -- temporary code to test promise stack
			resolve(nil) 
		end
		
		local chunk = self:_loadChunk(chunkId)
		
		if next(chunk._savedContent) == nil and next(chunk._virtualContent) == nil then -- check if we're at the last chunk
			resolve(nil)
		elseif chunk._savedContent[key] then	
			local isExpired = self._isTemporal and self:_isValueExpired(chunk._savedContent[key])
						
			if isExpired then
				self:_queueKeyRemoval(chunkId, key) 
				
				resolve(nil) -- since we resolve nil it'll seem like the data wasn't there in the first place
			else
				resolve(chunk._savedContent[key].Data) -- Testing out this over resolve(TableUtil.Copy(chunk._savedContent[key].Data, true)) 
			end
		elseif chunk._virtualContent[key] then -- virtual content is relatively recent we aren't too worried about it being expired
			resolve(chunk._virtualContent[key].Data)  -- Testing out this over resolve(TableUtil.Copy(chunk._virtualContent[key].Data, true)) 
		else
			resolve(self:Get(key, (chunkId or 0) + 1)) 
		end
	end)
end

return Chunking
